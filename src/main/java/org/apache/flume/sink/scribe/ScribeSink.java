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

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Synchronous Sink that forwards messages to a scribe listener. <p>
 * The use case for this sink is to maintain backward compatibility with scribe consumers when there is a desire
 * to migrate from Scribe middleware to FlumeNG.
 */
public class ScribeSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(ScribeSink.class);
    private long batchSize = 1;
    private SinkCounter sinkCounter;
    private FlumeEventSerializer serializer;
    private Scribe.Client client;
    private TTransport transport;

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

        try {
            transport = new TFramedTransport(new TSocket(new Socket(host, port)));
        }
        catch (Exception ex) {
            logger.error("Unable to create Thrift Transport", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        client = new Scribe.Client(new TBinaryProtocol(transport, false, false));
        sinkCounter.start();
    }

    @Override
    public synchronized void stop() {
        super.stop();
        sinkCounter.stop();
        transport.close();
        client = null;
    }

    @Override
    public Status process() throws EventDeliveryException {
        logger.debug("start processing");

        Status status = Status.READY;
        Channel channel = getChannel();
        List<LogEntry> eventList = new ArrayList<LogEntry>();
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        for (int i = 0; i < batchSize; i++) {
            Event event = channel.take();
            if(event == null) {
                status = Status.BACKOFF;
                sinkCounter.incrementBatchUnderflowCount();
                break;
            }
            else {
                eventList.add(serializer.serialize(event));
            }
        }

        sendEvents(transaction, eventList);
        return status;
    }

    private void sendEvents(Transaction transaction, List<LogEntry> eventList)
        throws EventDeliveryException {
        try {
            sinkCounter.addToEventDrainAttemptCount(eventList.size());
            ResultCode rc = client.Log(eventList);
            if (rc.equals(ResultCode.OK)) {
                transaction.commit();
                sinkCounter.addToEventDrainSuccessCount(eventList.size());
            }
        }
        catch (Throwable e) {
            transaction.rollback();
            logger.error("exception while processing in Scribe Sink", e);
            throw new EventDeliveryException("Failed to send message", e);
        }
        finally {
            transaction.close();
        }
    }
}
