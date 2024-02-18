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
package org.apache.rocketmq.store;

public enum PutMessageStatus {

    /**
     * 可以进行存储
     */
    PUT_OK,

    /**
     * 刷盘超时
     */
    FLUSH_DISK_TIMEOUT,

    FLUSH_SLAVE_TIMEOUT,
    SLAVE_NOT_AVAILABLE,
    /**
     * 服务不可用
     */
    SERVICE_NOT_AVAILABLE,

    /**
     * 创建 MappedFile (也就是 CommitLog) 文件失败
     */
    CREATE_MAPEDFILE_FAILED,

    /**
     * MQ 消息本身有异常, topic 名称太长或者属性值太多
     */
    MESSAGE_ILLEGAL,
    PROPERTIES_SIZE_EXCEEDED,
    /**
     * 操作系统页写入繁忙
     */
    OS_PAGECACHE_BUSY,

    /**
     * 未知异常
     */
    UNKNOWN_ERROR,

    /**
     * 多队列分发功能检测, 参数校验失败
     */
    LMQ_CONSUME_QUEUE_NUM_EXCEEDED,
}
