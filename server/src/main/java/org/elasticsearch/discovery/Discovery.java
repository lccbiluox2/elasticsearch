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

package org.elasticsearch.discovery;

import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.common.component.LifecycleComponent;

/**
 * A pluggable module allowing to implement discovery of other nodes, publishing of the cluster
 * state to all nodes, electing a master of the cluster that raises cluster state change
 * events.
 *
 * 一个可插入模块，允许发现其他节点，将集群状态发布到所有节点，选择引发集群状态更改事件的集群主节点。
 */
public interface Discovery extends LifecycleComponent, ClusterStatePublisher {

    /**
     * @return stats about the discovery
     */
    DiscoveryStats stats();

    /**
     * Triggers the first join cycle
     */
    void startInitialJoin();

}
