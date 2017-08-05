/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.weiboyi.etl.flume.source.canal;

public class CanalSourceConstants {

    public static final String ZOOKEEPER_SERVERS = "zkServers";
    public static final String SERVER_URL = "serverUrl";
    public static final String SERVER_URLS = "serverUrls";
    public static final String DESTINATION = "destination";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String BATCH_SIZE = "batchSize";
    public static final String FILTER = "filter";
    public static final String OLD_DATA_REQUIRED = "oldDataRequired";

    public static final int DEFAULT_BATCH_SIZE = 1024;
    public static final String DEFAULT_USERNAME = "";
    public static final String DEFAULT_PASSWORD = "";
    public static final boolean DEFAULT_OLD_DATA_REQUIRED = true;




}
