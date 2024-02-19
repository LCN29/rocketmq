/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Cycle average Hashing queue algorithm
 * <p>
 * 环形平均分配算法
 * 将所有的消费者按照圆环排列起来, 将队列每次一个分配给圆环的消费者, 分配完圆环转向下一个, 指定队列分配完
 * <p>
 * 假设队列大小是 8 (编号0-7), 消费者数量 3（编号0-2）
 * 分配结果
 * 消费者0：队列 0，3，6；
 * 消费者1：队列 1，4，7；
 * 消费者2：队列 2，5
 */
public class AllocateMessageQueueAveragelyByCircle extends AbstractAllocateMessageQueueStrategy {

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
                                       List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }

        int index = cidAll.indexOf(currentCID);
        for (int i = index; i < mqAll.size(); i++) {
            if (i % cidAll.size() == index) {
                result.add(mqAll.get(i));
            }
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG_BY_CIRCLE";
    }
}
