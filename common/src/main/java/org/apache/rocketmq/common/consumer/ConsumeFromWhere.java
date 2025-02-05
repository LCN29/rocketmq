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
package org.apache.rocketmq.common.consumer;

public enum ConsumeFromWhere {

    /**
     * 消费者组第一次启动时从最后的位置消费，后续再启动接着上次消费的进度开始消费
     */
    CONSUME_FROM_LAST_OFFSET,

    @Deprecated
    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
    @Deprecated
    CONSUME_FROM_MIN_OFFSET,
    @Deprecated
    CONSUME_FROM_MAX_OFFSET,

    /**
     * 消费者组第一次启动时从最开始的位置消费，后续再启动接着上次消费的进度开始消费 (默认消息会在 Broker 中保存一段时间)
     */
    CONSUME_FROM_FIRST_OFFSET,

    /**
     * 从指定的时间戳开始消费
     */
    CONSUME_FROM_TIMESTAMP,
}
