/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.AutoCreateAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.IngestActionForwarder;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * Groups bulk request items by shard, optionally creating non-existent indices and
 * delegates to {@link TransportShardBulkAction} for shard-level bulk execution
 */
public class TransportBulkAction extends HandledTransportAction<BulkRequest, BulkResponse> {

    private static final Logger logger = LogManager.getLogger(TransportBulkAction.class);

    private final ThreadPool threadPool;
    private final AutoCreateIndex autoCreateIndex;
    private final ClusterService clusterService;
    private final IngestService ingestService;
    private final TransportShardBulkAction shardBulkAction;
    private final LongSupplier relativeTimeProvider;
    private final IngestActionForwarder ingestForwarder;
    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private static final String DROPPED_ITEM_WITH_AUTO_GENERATED_ID = "auto-generated";

    @Inject
    public TransportBulkAction(ThreadPool threadPool, TransportService transportService,
                               ClusterService clusterService, IngestService ingestService,
                               TransportShardBulkAction shardBulkAction, NodeClient client,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               AutoCreateIndex autoCreateIndex) {
        this(threadPool, transportService, clusterService, ingestService, shardBulkAction, client, actionFilters,
            indexNameExpressionResolver, autoCreateIndex, System::nanoTime);
    }

    public TransportBulkAction(ThreadPool threadPool, TransportService transportService,
                               ClusterService clusterService, IngestService ingestService,
                               TransportShardBulkAction shardBulkAction, NodeClient client,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               AutoCreateIndex autoCreateIndex, LongSupplier relativeTimeProvider) {
        super(BulkAction.NAME, transportService, actionFilters, BulkRequest::new, ThreadPool.Names.WRITE);
        Objects.requireNonNull(relativeTimeProvider);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.ingestService = ingestService;
        this.shardBulkAction = shardBulkAction;
        this.autoCreateIndex = autoCreateIndex;
        this.relativeTimeProvider = relativeTimeProvider;
        this.ingestForwarder = new IngestActionForwarder(transportService);
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        clusterService.addStateApplier(this.ingestForwarder);
    }

    /**
     * Retrieves the {@link IndexRequest} from the provided {@link DocWriteRequest} for index or upsert actions.  Upserts are
     * modeled as {@link IndexRequest} inside the {@link UpdateRequest}. Ignores {@link org.elasticsearch.action.delete.DeleteRequest}'s
     *
     * @param docWriteRequest The request to find the {@link IndexRequest}
     * @return the found {@link IndexRequest} or {@code null} if one can not be found.
     */
    public static IndexRequest getIndexWriteRequest(DocWriteRequest docWriteRequest) {
        IndexRequest indexRequest = null;
        if (docWriteRequest instanceof IndexRequest) {
            indexRequest = (IndexRequest) docWriteRequest;
        } else if (docWriteRequest instanceof UpdateRequest) {
            UpdateRequest updateRequest = (UpdateRequest) docWriteRequest;
            indexRequest = updateRequest.docAsUpsert() ? updateRequest.doc() : updateRequest.upsertRequest();
        }
        return indexRequest;
    }

    /**
     * 其大概处理流程如下 :
     *
     * 1. 判断bulk请是否需要加锁
     * 2. 获取所有需要处理的索引名称 对应上面的Step 1
     * 3. 过滤出bulk操作中不存在的索引,对应上面的Step 2
     * 4. 调用shouldAutoCreate(String index, ClusterState state),根据当前集群状态,判断当前索引是否需要自动创建
     * 5. 如果bulk操作的所有索引都不需要创建索引,则直接执行批量请求,否则会先创建索引
     * 6. 创建索引的具体实现交给TransportCreateIndexAction来完成,而TransportCreateIndexAction是TransportMasterNodeAction的子类,在TransportMasterNodeAction中完成了索引创建,这里也说明创建索引操作都会在master节点上完成
     * 7. master节点会针对所有请求会包装成一个Task,随后将该task以及请求,调用masterOperation()方法交给
     * TransportCreateIndexAction来完成创建索引前的工作
     *
     * @param task
     * @param bulkRequest
     * @param listener
     */
    @Override
    protected void doExecute(Task task, BulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
        final long startTime = relativeTime();
        final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());

        boolean hasIndexRequestsWithPipelines = false;
        final Metadata metadata = clusterService.state().getMetadata();
        final Version minNodeVersion = clusterService.state().getNodes().getMinNodeVersion();
        for (DocWriteRequest<?> actionRequest : bulkRequest.requests) {
            // 判断是更新还是插入
            IndexRequest indexRequest = getIndexWriteRequest(actionRequest);
            if (indexRequest != null) {
                // Each index request needs to be evaluated, because this method also modifies the IndexRequest
                // 每个索引请求都需要进行评估，因为这个方法还会修改IndexRequest
                boolean indexRequestHasPipeline = resolvePipelines(actionRequest, indexRequest, metadata);
                hasIndexRequestsWithPipelines |= indexRequestHasPipeline;
            }

            if (actionRequest instanceof IndexRequest) {
                IndexRequest ir = (IndexRequest) actionRequest;
                // 检测是否支持ID根据相应的version
                ir.checkAutoIdWithOpTypeCreateSupportedByVersion(minNodeVersion);
                if (ir.getAutoGeneratedTimestamp() != IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP) {
                    throw new IllegalArgumentException("autoGeneratedTimestamp should not be set externally");
                }
            }
        }

        if (hasIndexRequestsWithPipelines) {
            // this method (doExecute) will be called again, but with the bulk requests updated from the ingest node processing but
            // also with IngestService.NOOP_PIPELINE_NAME on each request. This ensures that this on the second time through this method,
            // this path is never taken.
            // 这个方法(doExecute)将被再次调用，但同时也会使用IngestService更新来自摄取节点处理的批量请求。每个请求上的
            // NOOP_PIPELINE_NAME。这确保了在第二次通过这个方法，/这个路径不会被采用。
            try {
                if (Assertions.ENABLED) {
                    final boolean arePipelinesResolved = bulkRequest.requests()
                        .stream()
                        .map(TransportBulkAction::getIndexWriteRequest)
                        .filter(Objects::nonNull)
                        .allMatch(IndexRequest::isPipelineResolved);
                    assert arePipelinesResolved : bulkRequest;
                }

                // 如果是 IngestNode
                if (clusterService.localNode().isIngestNode()) {
                    processBulkIndexIngestRequest(task, bulkRequest, listener);
                } else {
                    // 否则就转发请求
                    ingestForwarder.forwardIngestRequest(BulkAction.INSTANCE, bulkRequest, listener);
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
            return;
        }

        if (needToCheck()) {
            // Attempt to create all the indices that we're going to need during the bulk before we start.
            // Step 1: collect all the indices in the request
            final Set<String> indices = bulkRequest.requests.stream()
                    // delete requests should not attempt to create the index (if the index does not
                    // exists), unless an external versioning is used
                .filter(request -> request.opType() != DocWriteRequest.OpType.DELETE
                        || request.versionType() == VersionType.EXTERNAL
                        || request.versionType() == VersionType.EXTERNAL_GTE)
                .map(DocWriteRequest::index)
                .collect(Collectors.toSet());
            /* Step 2: filter that to indices that don't exist and we can create. At the same time build a map of indices we can't create
             * that we'll use when we try to run the requests. */
            final Map<String, IndexNotFoundException> indicesThatCannotBeCreated = new HashMap<>();

            // 判断是否自动创建索引，若不存在则创建索引，直接跳入execute执行，否则由executeBulk执行处理请求
            Set<String> autoCreateIndices = new HashSet<>();
            ClusterState state = clusterService.state();
            for (String index : indices) {
                boolean shouldAutoCreate;
                try {
                    shouldAutoCreate = shouldAutoCreate(index, state);
                } catch (IndexNotFoundException e) {
                    shouldAutoCreate = false;
                    indicesThatCannotBeCreated.put(index, e);
                }
                if (shouldAutoCreate) {
                    autoCreateIndices.add(index);
                }
            }
            // Step 3: create all the indices that are missing, if there are any missing. start the bulk after all the creates come back.
            if (autoCreateIndices.isEmpty()) {
                executeBulk(task, bulkRequest, startTime, listener, responses, indicesThatCannotBeCreated);
            } else {
                final AtomicInteger counter = new AtomicInteger(autoCreateIndices.size());
                for (String index : autoCreateIndices) {
                    createIndex(index, bulkRequest.timeout(), minNodeVersion,
                        new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(CreateIndexResponse result) {
                            if (counter.decrementAndGet() == 0) {
                                threadPool.executor(ThreadPool.Names.WRITE).execute(
                                    () -> executeBulk(task, bulkRequest, startTime, listener, responses, indicesThatCannotBeCreated));
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (!(ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException)) {
                                // fail all requests involving this index, if create didn't work
                                for (int i = 0; i < bulkRequest.requests.size(); i++) {
                                    DocWriteRequest<?> request = bulkRequest.requests.get(i);
                                    if (request != null && setResponseFailureIfIndexMatches(responses, i, request, index, e)) {
                                        bulkRequest.requests.set(i, null);
                                    }
                                }
                            }
                            if (counter.decrementAndGet() == 0) {
                                executeBulk(task, bulkRequest, startTime, ActionListener.wrap(listener::onResponse, inner -> {
                                    inner.addSuppressed(e);
                                    listener.onFailure(inner);
                                }), responses, indicesThatCannotBeCreated);
                            }
                        }
                    });
                }
            }
        } else {
            executeBulk(task, bulkRequest, startTime, listener, responses, emptyMap());
        }
    }

    static boolean resolvePipelines(final DocWriteRequest<?> originalRequest, final IndexRequest indexRequest, final Metadata metadata) {
        // 返回此请求的管道是否已由协调节点解决。
        if (indexRequest.isPipelineResolved() == false) {
            final String requestPipeline = indexRequest.getPipeline();
            indexRequest.setPipeline(IngestService.NOOP_PIPELINE_NAME);
            indexRequest.setFinalPipeline(IngestService.NOOP_PIPELINE_NAME);
            String defaultPipeline = null;
            String finalPipeline = null;
            // start to look for default or final pipelines via settings found in the index meta data
            // 开始通过在索引元数据中找到的设置查找默认或最终管道
            IndexMetadata indexMetadata = metadata.indices().get(originalRequest.index());
            // check the alias for the index request (this is how normal index requests are modeled)
            // 检查索引请求的别名(这是对普通索引请求建模的方式)
            if (indexMetadata == null && indexRequest.index() != null) {
                IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(indexRequest.index());
                if (indexAbstraction != null) {
                    indexMetadata = indexAbstraction.getWriteIndex();
                }
            }
            // check the alias for the action request (this is how upserts are modeled)
            // 检查操作请求的别名(这是upserts建模的方式)
            if (indexMetadata == null && originalRequest.index() != null) {
                IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(originalRequest.index());
                if (indexAbstraction != null) {
                    indexMetadata = indexAbstraction.getWriteIndex();
                }
            }
            if (indexMetadata != null) {
                final Settings indexSettings = indexMetadata.getSettings();
                if (IndexSettings.DEFAULT_PIPELINE.exists(indexSettings)) {
                    // find the default pipeline if one is defined from an existing index setting
                    // 如果从现有的索引设置中定义了一个管道，则查找默认管道
                    defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(indexSettings);
                    indexRequest.setPipeline(defaultPipeline);
                }
                if (IndexSettings.FINAL_PIPELINE.exists(indexSettings)) {
                    // find the final pipeline if one is defined from an existing index setting
                    finalPipeline = IndexSettings.FINAL_PIPELINE.get(indexSettings);
                    indexRequest.setFinalPipeline(finalPipeline);
                }
            } else if (indexRequest.index() != null) {
                // the index does not exist yet (and this is a valid request), so match index
                // templates to look for pipelines in either a matching V2 template (which takes
                // precedence), or if a V2 template does not match, any V1 templates
                // 索引还不存在(这是一个有效的请求)，所以匹配索引模板以在匹配的V2模板(优先级)中查找管道，或者如果V2模板不匹配，
                // 则在任何V1模板中查找管道
                String v2Template = MetadataIndexTemplateService.findV2Template(metadata, indexRequest.index(), false);
                if (v2Template != null) {
                    Settings settings = MetadataIndexTemplateService.resolveSettings(metadata, v2Template);
                    if (defaultPipeline == null && IndexSettings.DEFAULT_PIPELINE.exists(settings)) {
                        defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(settings);
                        // we can not break in case a lower-order template has a final pipeline that we need to collect
                    }
                    if (finalPipeline == null && IndexSettings.FINAL_PIPELINE.exists(settings)) {
                        finalPipeline = IndexSettings.FINAL_PIPELINE.get(settings);
                        // we can not break in case a lower-order template has a default pipeline that we need to collect
                    }
                    indexRequest.setPipeline(defaultPipeline == null ? IngestService.NOOP_PIPELINE_NAME : defaultPipeline);
                    indexRequest.setFinalPipeline(finalPipeline == null ? IngestService.NOOP_PIPELINE_NAME : finalPipeline);
                } else {
                    // 查找索引模式与给定索引名称匹配的索引模板。在隐藏索引的情况下，匹配所有模式或全局模板的模板将不会返回。
                    List<IndexTemplateMetadata> templates =
                            MetadataIndexTemplateService.findV1Templates(metadata, indexRequest.index(), null);
                    assert (templates != null);
                    // order of templates are highest order first
                    for (final IndexTemplateMetadata template : templates) {
                        final Settings settings = template.settings();
                        if (defaultPipeline == null && IndexSettings.DEFAULT_PIPELINE.exists(settings)) {
                            defaultPipeline = IndexSettings.DEFAULT_PIPELINE.get(settings);
                            // we can not break in case a lower-order template has a final pipeline that we need to collect
                        }
                        if (finalPipeline == null && IndexSettings.FINAL_PIPELINE.exists(settings)) {
                            finalPipeline = IndexSettings.FINAL_PIPELINE.get(settings);
                            // we can not break in case a lower-order template has a default pipeline that we need to collect
                        }
                        if (defaultPipeline != null && finalPipeline != null) {
                            // we can break if we have already collected a default and final pipeline
                            break;
                        }
                    }
                    indexRequest.setPipeline(defaultPipeline == null ? IngestService.NOOP_PIPELINE_NAME : defaultPipeline);
                    indexRequest.setFinalPipeline(finalPipeline == null ? IngestService.NOOP_PIPELINE_NAME : finalPipeline);
                }
            }

            if (requestPipeline != null) {
                indexRequest.setPipeline(requestPipeline);
            }

            /*
             * We have to track whether or not the pipeline for this request has already been resolved. It can happen that the
             * pipeline for this request has already been derived yet we execute this loop again. That occurs if the bulk request
             * has been forwarded by a non-ingest coordinating node to an ingest node. In this case, the coordinating node will have
             * already resolved the pipeline for this request. It is important that we are able to distinguish this situation as we
             * can not double-resolve the pipeline because we will not be able to distinguish the case of the pipeline having been
             * set from a request pipeline parameter versus having been set by the resolution. We need to be able to distinguish
             * these cases as we need to reject the request if the pipeline was set by a required pipeline and there is a request
             * pipeline parameter too.
             *
             * 我们必须跟踪这个请求的管道是否已经被解决。可能发生的情况是，这个请求的管道已经派生，但我们再次执行这个循环。
             * 如果非摄取协调节点已将批量请求转发到摄取节点，则会发生这种情况。在本例中，协调节点将已经为该请求解析了管道。
             * 能够区分这种情况非常重要，因为我们不能对管道进行双重解析，因为我们无法区分管道是通过请求管道参数设置的还是通过
             * 解析设置的。我们需要能够区分这些情况，因为如果管道是由必需的管道设置的，并且也有一个请求管道参数，
             * 我们就需要拒绝请求。
             */
            indexRequest.isPipelineResolved(true);
        }


        // return whether this index request has a pipeline
        return IngestService.NOOP_PIPELINE_NAME.equals(indexRequest.getPipeline()) == false
            || IngestService.NOOP_PIPELINE_NAME.equals(indexRequest.getFinalPipeline()) == false;
    }

    boolean needToCheck() {
        return autoCreateIndex.needToCheck();
    }

    boolean shouldAutoCreate(String index, ClusterState state) {
        return autoCreateIndex.shouldAutoCreate(index, state);
    }

    void createIndex(String index,
                     TimeValue timeout,
                     Version minNodeVersion,
                     ActionListener<CreateIndexResponse> listener) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest();
        createIndexRequest.index(index);
        createIndexRequest.cause("auto(bulk api)");
        createIndexRequest.masterNodeTimeout(timeout);
        if (minNodeVersion.onOrAfter(Version.V_7_8_0)) {
            client.execute(AutoCreateAction.INSTANCE, createIndexRequest, listener);
        } else {
            client.admin().indices().create(createIndexRequest, listener);
        }
    }

    private boolean setResponseFailureIfIndexMatches(AtomicArray<BulkItemResponse> responses, int idx, DocWriteRequest<?> request,
                                                     String index, Exception e) {
        if (index.equals(request.index())) {
            responses.set(idx, new BulkItemResponse(idx, request.opType(), new BulkItemResponse.Failure(request.index(), request.type(),
                request.id(), e)));
            return true;
        }
        return false;
    }

    private long buildTookInMillis(long startTimeNanos) {
        return TimeUnit.NANOSECONDS.toMillis(relativeTime() - startTimeNanos);
    }

    /**
     * retries on retryable cluster blocks, resolves item requests,
     * constructs shard bulk requests and delegates execution to shard bulk action
     * */
    private final class BulkOperation extends ActionRunnable<BulkResponse> {
        private final Task task;
        private BulkRequest bulkRequest; // set to null once all requests are sent out
        private final AtomicArray<BulkItemResponse> responses;
        private final long startTimeNanos;
        private final ClusterStateObserver observer;
        private final Map<String, IndexNotFoundException> indicesThatCannotBeCreated;

        BulkOperation(Task task, BulkRequest bulkRequest, ActionListener<BulkResponse> listener, AtomicArray<BulkItemResponse> responses,
                long startTimeNanos, Map<String, IndexNotFoundException> indicesThatCannotBeCreated) {
            super(listener);
            this.task = task;
            this.bulkRequest = bulkRequest;
            this.responses = responses;
            this.startTimeNanos = startTimeNanos;
            this.indicesThatCannotBeCreated = indicesThatCannotBeCreated;
            this.observer = new ClusterStateObserver(clusterService, bulkRequest.timeout(), logger, threadPool.getThreadContext());
        }

        @Override
        protected void doRun() {
            assert bulkRequest != null;
            final ClusterState clusterState = observer.setAndGetObservedState();
            // 判断集群的是否block了读操作,如果是blocked，就会返回timeout
            if (handleBlockExceptions(clusterState)) {
                return;
            }
            final ConcreteIndices concreteIndices = new ConcreteIndices(clusterState, indexNameExpressionResolver);
            Metadata metadata = clusterState.metadata();
            for (int i = 0; i < bulkRequest.requests.size(); i++) {
                DocWriteRequest<?> docWriteRequest = bulkRequest.requests.get(i);
                //the request can only be null because we set it to null in the previous step, so it gets ignored
                if (docWriteRequest == null) {
                    continue;
                }
                if (addFailureIfIndexIsUnavailable(docWriteRequest, i, concreteIndices, metadata)) {
                    continue;
                }
                Index concreteIndex = concreteIndices.resolveIfAbsent(docWriteRequest);
                try {
                    switch (docWriteRequest.opType()) {
                        case CREATE:
                        case INDEX:
                            IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                            final IndexMetadata indexMetadata = metadata.index(concreteIndex);
                            MappingMetadata mappingMd = indexMetadata.mappingOrDefault();
                            Version indexCreated = indexMetadata.getCreationVersion();
                            indexRequest.resolveRouting(metadata);
                            // 如果是IndexRequest就调用IndexRequest.process方法，主要是为了解析出timestamp,routing,id,parent 等字段。
                            indexRequest.process(indexCreated, mappingMd, concreteIndex.getName());
                            break;
                        case UPDATE:
                            TransportUpdateAction.resolveAndValidateRouting(metadata, concreteIndex.getName(),
                                (UpdateRequest) docWriteRequest);
                            break;
                        case DELETE:
                            docWriteRequest.routing(metadata.resolveWriteIndexRouting(docWriteRequest.routing(), docWriteRequest.index()));
                            // check if routing is required, if so, throw error if routing wasn't specified
                            if (docWriteRequest.routing() == null && metadata.routingRequired(concreteIndex.getName())) {
                                throw new RoutingMissingException(concreteIndex.getName(), docWriteRequest.type(), docWriteRequest.id());
                            }
                            break;
                        default: throw new AssertionError("request type not supported: [" + docWriteRequest.opType() + "]");
                    }
                } catch (ElasticsearchParseException | IllegalArgumentException | RoutingMissingException e) {
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(concreteIndex.getName(), docWriteRequest.type(),
                        docWriteRequest.id(), e);
                    BulkItemResponse bulkItemResponse = new BulkItemResponse(i, docWriteRequest.opType(), failure);
                    responses.set(i, bulkItemResponse);
                    // make sure the request gets never processed again
                    bulkRequest.requests.set(i, null);
                }
            }

            // first, go over all the requests and create a ShardId -> Operations mapping
            // 接着对新形成的这个结构(ShardId -> List[BulkItemRequest])做循环，也就是针对每个ShardId里的数据进行统一处理。
            Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();
            for (int i = 0; i < bulkRequest.requests.size(); i++) {
                DocWriteRequest<?> request = bulkRequest.requests.get(i);
                if (request == null) {
                    continue;
                }
                String concreteIndex = concreteIndices.getConcreteIndex(request.index()).getName();

                // 获取每个request应该发送到的shardId(获取过程： request有routing就直接返回，如果没有，会先对id求一个hash
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, concreteIndex, request.id(),
                    request.routing()).shardId();

                // 有了ShardId,bulkRequest,List[BulkItemRequest]等信息后，遍历map 统一封装成BulkShardRequest，就是对属于同一ShardId的数据构建
                // 一个新的类似BulkRequest的对象。包含配置consistencyLevel和timeout。
                List<BulkItemRequest> shardRequests = requestsByShard.computeIfAbsent(shardId, shard -> new ArrayList<>());
                shardRequests.add(new BulkItemRequest(i, request));
            }

            if (requestsByShard.isEmpty()) {
                listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]),
                    buildTookInMillis(startTimeNanos)));
                return;
            }

            final AtomicInteger counter = new AtomicInteger(requestsByShard.size());
            String nodeId = clusterService.localNode().getId();
            for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
                final ShardId shardId = entry.getKey();
                final List<BulkItemRequest> requests = entry.getValue();
                BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, bulkRequest.getRefreshPolicy(),
                        requests.toArray(new BulkItemRequest[requests.size()]));
                bulkShardRequest.waitForActiveShards(bulkRequest.waitForActiveShards());
                bulkShardRequest.timeout(bulkRequest.timeout());
                bulkShardRequest.routedBasedOnClusterVersion(clusterState.version());
                if (task != null) {
                    bulkShardRequest.setParentTask(nodeId, task.getId());
                }

                // 这里的shardBulkAction 是 TransportShardBulkAction
                shardBulkAction.execute(bulkShardRequest, new ActionListener<BulkShardResponse>() {
                    @Override
                    public void onResponse(BulkShardResponse bulkShardResponse) {
                        for (BulkItemResponse bulkItemResponse : bulkShardResponse.getResponses()) {
                            // we may have no response if item failed
                            if (bulkItemResponse.getResponse() != null) {
                                bulkItemResponse.getResponse().setShardInfo(bulkShardResponse.getShardInfo());
                            }
                            responses.set(bulkItemResponse.getItemId(), bulkItemResponse);
                        }
                        if (counter.decrementAndGet() == 0) {
                            finishHim();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // create failures for all relevant requests
                        for (BulkItemRequest request : requests) {
                            final String indexName = concreteIndices.getConcreteIndex(request.index()).getName();
                            DocWriteRequest<?> docWriteRequest = request.request();
                            responses.set(request.id(), new BulkItemResponse(request.id(), docWriteRequest.opType(),
                                    new BulkItemResponse.Failure(indexName, docWriteRequest.type(), docWriteRequest.id(), e)));
                        }
                        if (counter.decrementAndGet() == 0) {
                            finishHim();
                        }
                    }

                    private void finishHim() {
                        listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]),
                            buildTookInMillis(startTimeNanos)));
                    }
                });
            }
            bulkRequest = null; // allow memory for bulk request items to be reclaimed before all items have been completed
        }

        private boolean handleBlockExceptions(ClusterState state) {
            // 判断集群的是否block了读操作,如果是blocked，就会返回timeout
            ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
            if (blockException != null) {
                if (blockException.retryable()) {
                    logger.trace("cluster is blocked, scheduling a retry", blockException);
                    retry(blockException);
                } else {
                    onFailure(blockException);
                }
                return true;
            }
            return false;
        }

        void retry(Exception failure) {
            assert failure != null;
            if (observer.isTimedOut()) {
                // we running as a last attempt after a timeout has happened. don't retry
                onFailure(failure);
                return;
            }
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    run();
                }

                @Override
                public void onClusterServiceClose() {
                    onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    // Try one more time...
                    run();
                }
            });
        }

        private boolean addFailureIfIndexIsUnavailable(DocWriteRequest<?> request, int idx, final ConcreteIndices concreteIndices,
                final Metadata metadata) {
            IndexNotFoundException cannotCreate = indicesThatCannotBeCreated.get(request.index());
            if (cannotCreate != null) {
                addFailure(request, idx, cannotCreate);
                return true;
            }
            Index concreteIndex = concreteIndices.getConcreteIndex(request.index());
            if (concreteIndex == null) {
                try {
                    concreteIndex = concreteIndices.resolveIfAbsent(request);
                } catch (IndexClosedException | IndexNotFoundException ex) {
                    addFailure(request, idx, ex);
                    return true;
                }
            }
            IndexMetadata indexMetadata = metadata.getIndexSafe(concreteIndex);
            if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                addFailure(request, idx, new IndexClosedException(concreteIndex));
                return true;
            }
            return false;
        }

        private void addFailure(DocWriteRequest<?> request, int idx, Exception unavailableException) {
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(request.index(), request.type(), request.id(),
                    unavailableException);
            BulkItemResponse bulkItemResponse = new BulkItemResponse(idx, request.opType(), failure);
            responses.set(idx, bulkItemResponse);
            // make sure the request gets never processed again
            bulkRequest.requests.set(idx, null);
        }
    }

    void executeBulk(Task task, final BulkRequest bulkRequest, final long startTimeNanos, final ActionListener<BulkResponse> listener,
            final AtomicArray<BulkItemResponse> responses, Map<String, IndexNotFoundException> indicesThatCannotBeCreated) {
        new BulkOperation(task, bulkRequest, listener, responses, startTimeNanos, indicesThatCannotBeCreated).run();
    }

    private static class ConcreteIndices  {
        private final ClusterState state;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final Map<String, Index> indices = new HashMap<>();

        ConcreteIndices(ClusterState state, IndexNameExpressionResolver indexNameExpressionResolver) {
            this.state = state;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
        }

        Index getConcreteIndex(String indexOrAlias) {
            return indices.get(indexOrAlias);
        }

        Index resolveIfAbsent(DocWriteRequest<?> request) {
            Index concreteIndex = indices.get(request.index());
            if (concreteIndex == null) {
                boolean includeDataStreams = request.opType() == DocWriteRequest.OpType.CREATE;
                concreteIndex = indexNameExpressionResolver.concreteWriteIndex(state, request, includeDataStreams);
                indices.put(request.index(), concreteIndex);
            }
            return concreteIndex;
        }
    }

    private long relativeTime() {
        return relativeTimeProvider.getAsLong();
    }

    private void processBulkIndexIngestRequest(Task task, BulkRequest original, ActionListener<BulkResponse> listener) {
        final long ingestStartTimeInNanos = System.nanoTime();
        final BulkRequestModifier bulkRequestModifier = new BulkRequestModifier(original);
        ingestService.executeBulkRequest(
            original.numberOfActions(),
            () -> bulkRequestModifier,
            bulkRequestModifier::markItemAsFailed,
            (originalThread, exception) -> {
                if (exception != null) {
                    logger.debug("failed to execute pipeline for a bulk request", exception);
                    listener.onFailure(exception);
                } else {
                    long ingestTookInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ingestStartTimeInNanos);
                    BulkRequest bulkRequest = bulkRequestModifier.getBulkRequest();
                    ActionListener<BulkResponse> actionListener = bulkRequestModifier.wrapActionListenerIfNeeded(ingestTookInMillis,
                        listener);
                    if (bulkRequest.requests().isEmpty()) {
                        // at this stage, the transport bulk action can't deal with a bulk request with no requests,
                        // so we stop and send an empty response back to the client.
                        // (this will happen if pre-processing all items in the bulk failed)
                        actionListener.onResponse(new BulkResponse(new BulkItemResponse[0], 0));
                    } else {
                        // If a processor went async and returned a response on a different thread then
                        // before we continue the bulk request we should fork back on a write thread:
                        if (originalThread == Thread.currentThread()) {
                            assert Thread.currentThread().getName().contains(ThreadPool.Names.WRITE);
                            doExecute(task, bulkRequest, actionListener);
                        } else {
                            threadPool.executor(ThreadPool.Names.WRITE).execute(new AbstractRunnable() {
                                @Override
                                public void onFailure(Exception e) {
                                    listener.onFailure(e);
                                }

                                @Override
                                protected void doRun() throws Exception {
                                    doExecute(task, bulkRequest, actionListener);
                                }

                                @Override
                                public boolean isForceExecution() {
                                    // If we fork back to a write thread we **not** should fail, because tp queue is full.
                                    // (Otherwise the work done during ingest will be lost)
                                    // It is okay to force execution here. Throttling of write requests happens prior to
                                    // ingest when a node receives a bulk request.
                                    return true;
                                }
                            });
                        }
                    }
                }
            },
            bulkRequestModifier::markItemAsDropped
        );
    }

    static final class BulkRequestModifier implements Iterator<DocWriteRequest<?>> {

        private static final Logger logger = LogManager.getLogger(BulkRequestModifier.class);

        final BulkRequest bulkRequest;
        final SparseFixedBitSet failedSlots;
        final List<BulkItemResponse> itemResponses;
        final AtomicIntegerArray originalSlots;

        volatile int currentSlot = -1;

        BulkRequestModifier(BulkRequest bulkRequest) {
            this.bulkRequest = bulkRequest;
            this.failedSlots = new SparseFixedBitSet(bulkRequest.requests().size());
            this.itemResponses = new ArrayList<>(bulkRequest.requests().size());
            this.originalSlots = new AtomicIntegerArray(bulkRequest.requests().size()); // oversize, but that's ok
        }

        @Override
        public DocWriteRequest<?> next() {
            return bulkRequest.requests().get(++currentSlot);
        }

        @Override
        public boolean hasNext() {
            return (currentSlot + 1) < bulkRequest.requests().size();
        }

        BulkRequest getBulkRequest() {
            if (itemResponses.isEmpty()) {
                return bulkRequest;
            } else {
                BulkRequest modifiedBulkRequest = new BulkRequest();
                modifiedBulkRequest.setRefreshPolicy(bulkRequest.getRefreshPolicy());
                modifiedBulkRequest.waitForActiveShards(bulkRequest.waitForActiveShards());
                modifiedBulkRequest.timeout(bulkRequest.timeout());

                int slot = 0;
                List<DocWriteRequest<?>> requests = bulkRequest.requests();
                for (int i = 0; i < requests.size(); i++) {
                    DocWriteRequest<?> request = requests.get(i);
                    if (failedSlots.get(i) == false) {
                        modifiedBulkRequest.add(request);
                        originalSlots.set(slot++, i);
                    }
                }
                return modifiedBulkRequest;
            }
        }

        ActionListener<BulkResponse> wrapActionListenerIfNeeded(long ingestTookInMillis, ActionListener<BulkResponse> actionListener) {
            if (itemResponses.isEmpty()) {
                return ActionListener.map(actionListener,
                    response -> new BulkResponse(response.getItems(), response.getTook().getMillis(), ingestTookInMillis));
            } else {
                return ActionListener.delegateFailure(actionListener, (delegatedListener, response) -> {
                    BulkItemResponse[] items = response.getItems();
                    for (int i = 0; i < items.length; i++) {
                        itemResponses.add(originalSlots.get(i), response.getItems()[i]);
                    }
                    delegatedListener.onResponse(
                        new BulkResponse(
                            itemResponses.toArray(new BulkItemResponse[0]), response.getTook().getMillis(), ingestTookInMillis));
                });
            }
        }

        synchronized void markItemAsDropped(int slot) {
            IndexRequest indexRequest = getIndexWriteRequest(bulkRequest.requests().get(slot));
            failedSlots.set(slot);
            final String id = indexRequest.id() == null ? DROPPED_ITEM_WITH_AUTO_GENERATED_ID : indexRequest.id();
            itemResponses.add(
                new BulkItemResponse(slot, indexRequest.opType(),
                    new UpdateResponse(
                        new ShardId(indexRequest.index(), IndexMetadata.INDEX_UUID_NA_VALUE, 0),
                        indexRequest.type(), id, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                        indexRequest.version(), DocWriteResponse.Result.NOOP
                    )
                )
            );
        }

        synchronized void markItemAsFailed(int slot, Exception e) {
            IndexRequest indexRequest = getIndexWriteRequest(bulkRequest.requests().get(slot));
            logger.debug(() -> new ParameterizedMessage("failed to execute pipeline [{}] for document [{}/{}/{}]",
                indexRequest.getPipeline(), indexRequest.index(), indexRequest.type(), indexRequest.id()), e);

            // We hit a error during preprocessing a request, so we:
            // 1) Remember the request item slot from the bulk, so that we're done processing all requests we know what failed
            // 2) Add a bulk item failure for this request
            // 3) Continue with the next request in the bulk.
            failedSlots.set(slot);
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(indexRequest.index(), indexRequest.type(),
                indexRequest.id(), e);
            itemResponses.add(new BulkItemResponse(slot, indexRequest.opType(), failure));
        }

    }
}
