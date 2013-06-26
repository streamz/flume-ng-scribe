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

import com.google.common.base.Throwables;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Asynchronous Sink that forwards messages to a scribe listener. <p>
 * The use case for this sink is to maintain backward compatibility with scribe consumers when there is a desire
 * to migrate from Scribe middleware to FlumeNG.
 */
public class AsyncScribeSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(ScribeSink.class);
    private long batchSize = 1;
    private SinkCounter sinkCounter;
    private FlumeEventSerializer serializer;
    private Scribe.AsyncClient client;
    private TAsyncClientManager clientManager;
    private TNonblockingTransport transport;
    private long timeout;

    @Override
    public void configure(Context context) {
        String name = context.getString(ScribeSinkConfigurationConstants.CONFIG_SINK_NAME, "sink-" + hashCode());
        setName(name);
        sinkCounter = new SinkCounter(name);
        batchSize = context.getLong(ScribeSinkConfigurationConstants.CONFIG_BATCHSIZE, 1L);
        String clazz = context.getString(ScribeSinkConfigurationConstants.CONFIG_SERIALIZER, EventToLogEntrySerializer.class.getName());

        try {
            serializer = (FlumeEventSerializer)Class.forName(clazz).newInstance();
        }
        catch (Exception ex) {
            logger.warn("Defaulting to EventToLogEntrySerializer", ex);
            serializer = new EventToLogEntrySerializer();
        }
        finally {
            serializer.configure(context);
        }

        String host = context.getString(ScribeSinkConfigurationConstants.CONFIG_SCRIBE_HOST);
        int port = context.getInteger(ScribeSinkConfigurationConstants.CONFIG_SCRIBE_PORT);
        timeout = context.getInteger(ScribeSinkConfigurationConstants.CONFIG_SCRIBE_TIMEOUT, 1000);

        try {
            transport = new TNonblockingSocket(host, port);
            clientManager = new TAsyncClientManager();
        }
        catch (Exception ex) {
            logger.error("Unable to create Thrift Transport", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        sinkCounter.start();
        client = new Scribe.AsyncClient(new TBinaryProtocol.Factory(), clientManager, transport);
    }

    @Override
    public synchronized void stop() {
        super.stop();
        sinkCounter.stop();
        clientManager.stop();
        transport.close();
        client = null;
    }

    @Override
    public Status process() throws EventDeliveryException {
        logger.debug("start processing");
        AtomicBoolean txnFail = new AtomicBoolean(false);
        AtomicInteger callbacksReceived = new AtomicInteger(0);
        AtomicInteger callbacksExpected = new AtomicInteger(0);
        final Lock lock = new ReentrantLock();
        final Condition condition = lock.newCondition();
        final AsyncCallBack<Scribe.AsyncClient.Log_call> cb = new AsyncCallBack(lock, condition, callbacksReceived, txnFail);
        Status status = Status.READY;
        Channel channel = getChannel();
        List<LogEntry> eventList = new ArrayList<LogEntry>();
        Transaction transaction = null;
        int i = 0;

        try {
            transaction = channel.getTransaction();
            transaction.begin();

            for (; i < batchSize; i++) {
                Event event = channel.take();
                if(event == null) {
                    status = Status.BACKOFF;
                    if (i == 0) {
                        sinkCounter.incrementBatchEmptyCount();
                    }
                    else {
                        sinkCounter.incrementBatchUnderflowCount();
                    }
                    break;
                }
                else {
                    eventList.add(serializer.serialize(event));
                    callbacksExpected.set(eventList.size());
                    client.Log(eventList, cb);
                }
            }
        }
        catch (Throwable e) {
            handleTransactionFailure(transaction);
            checkIfChannelExceptionAndThrow(e);
        }

        if (i == batchSize) {
            sinkCounter.incrementBatchCompleteCount();
        }

        sinkCounter.addToEventDrainAttemptCount(i);
        lock.lock();

        try {
            while ((callbacksReceived.get() < callbacksExpected.get()) && !txnFail.get()) {
                try {
                    if(!condition.await(timeout, TimeUnit.MILLISECONDS)){
                        txnFail.set(true);
                        logger.warn("Thrift callback timed out. Transaction will be rolled back.");
                    }
                }
                catch (Exception ex) {
                    logger.error("Exception while waiting for callbacks from Thrift.");
                    this.handleTransactionFailure(transaction);
                    Throwables.propagate(ex);
                }
            }
        }
        finally {
            lock.unlock();
        }

        /*
         * At this point, either the txn has failed
         * or all callbacks received and txn is successful.
         *
         * This need not be in the monitor, since all callbacks for this txn
         * have been received. So txnFail will not be modified any more(even if
         * it is, it is set from true to true only - false happens only
         * in the next process call).
         *
         */
        if (txnFail.get()) {
            this.handleTransactionFailure(transaction);
            throw new EventDeliveryException("Could not write events to Scribe. Transaction failed, and rolled back.");
        }
        else {
            try{
                transaction.commit();
                transaction.close();
                sinkCounter.addToEventDrainSuccessCount(i);
            }
            catch (Throwable e) {
                this.handleTransactionFailure(transaction);
                this.checkIfChannelExceptionAndThrow(e);
            }
        }

        return status;
    }

    private void handleTransactionFailure(Transaction txn) throws EventDeliveryException {
        try {
            txn.rollback();
        }
        catch (Throwable e) {
            logger.error("Failed to commit transaction. Transaction rolled back.", e);
            if(e instanceof Error || e instanceof RuntimeException) {
                logger.error("Failed to commit transaction. Transaction rolled back.", e);
                Throwables.propagate(e);
            } else {
                logger.error("Failed to commit transaction. Transaction rolled back.", e);
                throw new EventDeliveryException("Failed to commit transaction. Transaction rolled back.", e);
            }
        }
        finally {
            txn.close();
        }
    }

    private void checkIfChannelExceptionAndThrow(Throwable e) throws EventDeliveryException {
        if (e instanceof ChannelException) {
            throw new EventDeliveryException("Error in processing transaction.", e);
        }
        else if (e instanceof Error || e instanceof RuntimeException) {
            Throwables.propagate(e);
        }
        throw new EventDeliveryException("Error in processing transaction.", e);
    }
}
