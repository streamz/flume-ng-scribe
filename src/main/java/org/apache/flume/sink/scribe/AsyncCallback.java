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

import org.apache.thrift.async.AsyncMethodCallback;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

class AsyncCallBack<T> implements AsyncMethodCallback<T> {
    private final Lock lock;
    private final AtomicInteger callbacksReceived;
    private final Condition condition;
    private final AtomicBoolean txnFail;

    public AsyncCallBack(Lock lock, Condition condition, AtomicInteger callbacksReceived, AtomicBoolean txnFail) {
        this.lock = lock;
        this.condition = condition;
        this.callbacksReceived = callbacksReceived;
        this.txnFail = txnFail;
    }

    @Override
    public void onComplete(T call) {
        callbacksReceived.incrementAndGet();
        txnFail.set(false);
        lock.lock();
        try {
            condition.signal();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void onError(Exception e) {
        callbacksReceived.incrementAndGet();
        txnFail.set(true);
    }
}
