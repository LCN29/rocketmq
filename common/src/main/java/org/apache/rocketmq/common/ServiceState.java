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
package org.apache.rocketmq.common;

public enum ServiceState {
    /**
     * Service just created,not start
     * 服务仅创建, 但是未启动
     */
    CREATE_JUST,

    /**
     * Service Running
     * 服务执行中
     */
    RUNNING,

    /**
     * Service shutdown
     * 服务关闭
     */
    SHUTDOWN_ALREADY,

    /**
     * Service Start failure
     * 服务启动失败
     */
    START_FAILED;
}
