/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.scribe;

public class ScribeSinkConfigurationConstants {
    /**
     * Maximum number of events the sink should take from the channel per
     * transaction, if available.
     */
    public static final String CONFIG_BATCHSIZE = "batchSize";
    /**
     * The fully qualified class name of the serializer the sink should use.
     */
    public static final String CONFIG_SERIALIZER = "serializer";
    /**
     * The name the sink should use.
     */
    public static final String CONFIG_SINK_NAME = "scribe.sink.name";
    /**
     * Scribe host to send to.
     */
    public static final String CONFIG_SCRIBE_HOST = "scribe.host";
    /**
     * Scribe port to connect to.
     */
    public static final String CONFIG_SCRIBE_PORT = "scribe.port";
    /**
     * Scribe port to connect to.
     */
    public static final String CONFIG_SCRIBE_TIMEOUT = "scribe.timeout";
    /**
     * Flume Header Key that maps to a Scribe Category.
     */
    public static final String CONFIG_SCRIBE_CATEGORY_HEADER = "scribe.category.header";
    /**
     * Flume Header Value that maps to a Scribe Category, used for tests.
     */
    static final String CONFIG_SCRIBE_CATEGORY = "scribe.category";

    private ScribeSinkConfigurationConstants() {
    }
}
