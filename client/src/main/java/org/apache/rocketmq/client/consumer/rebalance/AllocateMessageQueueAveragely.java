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
 * Average Hashing queue algorithm
 *
 * 平均分配算法
 *
 * 根据 Topic 下所有的队列 除以 消费者组中的所有实例个数 得到一个平均数
 * 然后根据这个平均数, 将对应的队列连续的分配给一个消费者实例
 *
 * 假设队列大小是 8 (编号0-7), 消费者数量 3（编号0-2）
 * 分配结果
 * 消费者0：队列 0，1，2；
 * 消费者1：队列 3，4，5；
 * 消费者2：队列 6，7
 *
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueStrategy {

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
                                       List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        // 客户端 id 校验, 不符合就返回空
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }
        // 获取当前的客户端在所有客户端中的位置, 也就是第几个
        int index = cidAll.indexOf(currentCID);
        // 取模, 得到最终的余数
        int mod = mqAll.size() % cidAll.size();
        /* int averageSize =
                mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                        + 1 : mqAll.size() / cidAll.size());*/

        // 设置默认值, 客户端的数量大于 MQ 队列的数量, 客户端默认可以分配到的平均队列数量为 1 (有的客户端没有队列可以消费)
        int averageSize = 1;
        if (mqAll.size() > cidAll.size()) {
            // mod 大于 0, 表示不能完整除尽, 那么当前客户端的在所有客户端的前 mod 个, 会多消费 1 个, 其他的就是平均值了
            // key 看上面的假设例子
            averageSize = mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 : mqAll.size() / cidAll.size();
        }

        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }

}
