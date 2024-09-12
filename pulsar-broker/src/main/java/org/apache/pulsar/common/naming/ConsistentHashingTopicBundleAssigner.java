/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.naming;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import org.apache.pulsar.broker.PulsarService;

public class ConsistentHashingTopicBundleAssigner implements TopicBundleAssignmentStrategy {
    @Override
    public NamespaceBundle findBundle(TopicName topicName, NamespaceBundles namespaceBundles) {
        //计算Topic名称的哈希值
        long hashCode = Hashing.crc32().hashString(topicName.toString(), StandardCharsets.UTF_8).padToLong();
        //根据哈希值来获取所归属的bundle，一致性哈希的设计
        NamespaceBundle bundle = namespaceBundles.getBundle(hashCode);
        if (topicName.getDomain().equals(TopicDomain.non_persistent)) {
            bundle.setHasNonPersistentTopic(true);
        }
        return bundle;
    }

    @Override
    public void init(PulsarService pulsarService) {
    }

}
