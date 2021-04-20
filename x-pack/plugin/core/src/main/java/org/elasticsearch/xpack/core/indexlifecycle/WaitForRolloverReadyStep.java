/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Waits for at least one rollover condition to be satisfied, using the Rollover API's dry_run option.
 *
 * 使用rollover API的dry_run选项，等待至少一个翻转条件得到满足。
 */
public class WaitForRolloverReadyStep extends AsyncWaitStep {
    private static final Logger logger = LogManager.getLogger(WaitForRolloverReadyStep.class);

    public static final String NAME = "check-rollover-ready";

    private final ByteSizeValue maxSize;
    private final TimeValue maxAge;
    private final Long maxDocs;

    public WaitForRolloverReadyStep(StepKey key, StepKey nextStepKey, Client client, ByteSizeValue maxSize, TimeValue maxAge,
                                    Long maxDocs) {
        super(key, nextStepKey, client);
        this.maxSize = maxSize;
        this.maxAge = maxAge;
        this.maxDocs = maxDocs;
    }

    @Override
    public void evaluateCondition(IndexMetaData indexMetaData, Listener listener) {
        String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(indexMetaData.getSettings());

        if (Strings.isNullOrEmpty(rolloverAlias)) {
            listener.onFailure(new IllegalArgumentException(String.format(Locale.ROOT,
                "setting [%s] for index [%s] is empty or not defined", RolloverAction.LIFECYCLE_ROLLOVER_ALIAS,
                indexMetaData.getIndex().getName())));
            return;
        }

        // The order of the following checks is important in ways which may not be obvious.

        // First, figure out if 1) The configured alias points to this index, and if so,
        // whether this index is the write alias for this index
        // 首先，判断是否1) 配置的别名指向这个索引，如果是，这个索引是否为这个索引的写别名
        boolean aliasPointsToThisIndex = indexMetaData.getAliases().containsKey(rolloverAlias);

        Boolean isWriteIndex = null;
        // 如果别名指向该索引,判断该索引是不是可写状态
        if (aliasPointsToThisIndex) {
            // The writeIndex() call returns a tri-state boolean:
            // true  -> this index is the write index for this alias
            // false -> this index is not the write index for this alias
            // null  -> this alias is a "classic-style" alias and does not have a write index configured, but only points to one index
            //          and is thus the write index by default
            // true ->该索引是该别名的写索引
            // false ->该索引不是该别名的写索引
            // null ->这个别名是一个“经典风格”的别名，没有配置写索引，但只指向一个索引，因此在默认情况下是写索引
            isWriteIndex = indexMetaData.getAliases().get(rolloverAlias).writeIndex();
        }

        // 然后判断索引是否是已经完成状态 index.lifecycle.indexing_complete
        boolean indexingComplete = LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.get(indexMetaData.getSettings());
        if (indexingComplete) {
            logger.trace(indexMetaData.getIndex() + " has lifecycle complete set, skipping " + WaitForRolloverReadyStep.NAME);
            // If this index is still the write index for this alias, skipping rollover and continuing with the policy almost certainly
            // isn't what we want, as something likely still expects to be writing to this index.
            // If the alias doesn't point to this index, that's okay as that will be the result if this index is using a
            // "classic-style" alias and has already rolled over, and we want to continue with the policy.

            // 如果这个索引仍然是这个别名的写索引，跳过滚转并继续执行策略几乎肯定不是我们想要的，因为仍然可能有东西希望写入这个索引。
            // 如果别名没有指向该索引，那也没关系，因为如果该索引使用“经典风格”别名并且已经翻转，那么结果就是这样，我们想继续使用策略。
            if (aliasPointsToThisIndex && Boolean.TRUE.equals(isWriteIndex)) {
                listener.onFailure(new IllegalStateException(String.format(Locale.ROOT,
                    "index [%s] has [%s] set to [true], but is still the write index for alias [%s]",
                    indexMetaData.getIndex().getName(), LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, rolloverAlias)));
                return;
            }

            listener.onResponse(true, new WaitForRolloverReadyStep.EmptyInfo());
            return;
        }

        // If indexing_complete is *not* set, and the alias does not point to this index, we can't roll over this index, so error out.
        // 如果indexing_complete是 *not* set，并且别名没有指向这个索引，我们就不能滚过这个索引，因此错误输出。
        if (aliasPointsToThisIndex == false) {
            listener.onFailure(new IllegalArgumentException(String.format(Locale.ROOT,
                "%s [%s] does not point to index [%s]", RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, rolloverAlias,
                indexMetaData.getIndex().getName())));
            return;
        }

        // Similarly, if isWriteIndex is false (see note above on false vs. null), we can't roll over this index, so error out.
        if (Boolean.FALSE.equals(isWriteIndex)) {
            listener.onFailure(new IllegalArgumentException(String.format(Locale.ROOT,
                "index [%s] is not the write index for alias [%s]", indexMetaData.getIndex().getName(), rolloverAlias)));
        }

        RolloverRequest rolloverRequest = new RolloverRequest(rolloverAlias, null);
        rolloverRequest.dryRun(true);
        if (maxAge != null) {
            rolloverRequest.addMaxIndexAgeCondition(maxAge);
        }
        if (maxSize != null) {
            rolloverRequest.addMaxIndexSizeCondition(maxSize);
        }
        if (maxDocs != null) {
            rolloverRequest.addMaxIndexDocsCondition(maxDocs);
        }
        getClient().admin().indices().rolloverIndex(rolloverRequest,
            ActionListener.wrap(response -> listener.onResponse(response.getConditionStatus().values().stream().anyMatch(i -> i),
                new WaitForRolloverReadyStep.EmptyInfo()), listener::onFailure));
    }

    ByteSizeValue getMaxSize() {
        return maxSize;
    }

    TimeValue getMaxAge() {
        return maxAge;
    }

    Long getMaxDocs() {
        return maxDocs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxSize, maxAge, maxDocs);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        WaitForRolloverReadyStep other = (WaitForRolloverReadyStep) obj;
        return super.equals(obj) &&
            Objects.equals(maxSize, other.maxSize) &&
            Objects.equals(maxAge, other.maxAge) &&
            Objects.equals(maxDocs, other.maxDocs);
    }

    // We currently have no information to provide for this AsyncWaitStep, so this is an empty object
    private class EmptyInfo implements ToXContentObject {
        private EmptyInfo() {
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }
}
