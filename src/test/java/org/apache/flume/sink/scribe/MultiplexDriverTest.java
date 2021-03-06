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

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * Not really a unit test
 */
public class MultiplexDriverTest {
    @Test
    public void testIt() throws Exception {
        RpcClient rpcClient = RpcClientFactory.getDefaultInstance("127.0.0.1", 4444);
        String[] events = {"a", "b", "c"};
        String[] cat = {"default1", "default1", "default1"};

        for (int i = 0; i < 10000; ++i) {
            Map<String, String> hdrs = new HashMap<String, String>();
            hdrs.put("type", "event-" + events[i % events.length]);
            hdrs.put("cat", cat[i % cat.length]);
            Event flumeEvent = EventBuilder.withBody("test event " + i, Charset.forName("UTF8"), hdrs);
            rpcClient.append(flumeEvent);
        }

        rpcClient.close();
    }
}
