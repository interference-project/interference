/**
 The MIT License (MIT)

 Copyright (c) 2010-2021 head systems, ltd

 Permission is hereby granted, free of charge, to any person obtaining a copy of
 this software and associated documentation files (the "Software"), to deal in
 the Software without restriction, including without limitation the rights to
 use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 the Software, and to permit persons to whom the Software is furnished to do so,
 subject to the following conditions:

 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

package su.interference.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.api.TransportApi;
import su.interference.core.Config;
import su.interference.exception.InternalException;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class TransportContext implements TransportApi {

    private final static Logger logger = LoggerFactory.getLogger(TransportContext.class);
    private static TransportContext transportContext;
    private final AtomicBoolean started = new AtomicBoolean(true);
    private final ConcurrentLinkedQueue<TransportEvent> mq = new ConcurrentLinkedQueue<>();
    private final LinkedBlockingQueue<TransportMessage> inq = new LinkedBlockingQueue<>(10000);
    private final ExecutorService pool = Executors.newFixedThreadPool(1);
    private final ExecutorService pool2 = Executors.newFixedThreadPool(2);
    private final Map<String, TransportMessage> mmap = new ConcurrentHashMap<>();
    private final int callbackPort;
    private final TransportChannel clientChannel;
    private TransportServer transportServer;
    protected static final String CHANNEL_FAILURE_MESSAGE = "Channel failure";
    protected static final String TRANSACTION_ISNULL_MESSAGE = "Transaction is null";
    protected static final String WRONG_CALLBACK_NODE_MESSAGE = "Wrong callback node id";
    protected static final String UNABLE_LOCK_TABLE_MESSAGE = "Unable to lock table";
    protected static final String UNABLE_UNLOCK_TABLE_MESSAGE = "Unable to unlock table";
    protected static final String UNABLE_LOCK_FRAME_MESSAGE = "Unable to lock frame";
    protected static final String UNABLE_UNLOCK_FRAME_MESSAGE = "Unable to unlock frame";

    private TransportContext() {
        this.clientChannel = null;
        this.callbackPort = 0;
    }

    private TransportContext(TransportChannel clientChannel, int callbackPort) {
        this.clientChannel = clientChannel;
        this.callbackPort = callbackPort;
    }

    public void start() {
        startServer();
        startIncomingMessageProcess();
        startClient();
    }

    public void stop() {
        started.set(false);
        if (transportServer != null) {
            transportServer.stop();
        }
    }

    private void startServer() {
        if (clientChannel != null && callbackPort > 0) {
            transportServer = TransportServer.getInstance(callbackPort);
        } else {
            transportServer = TransportServer.getInstance();
        }
    }

    private void startClient() {
        pool.submit(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName("interference-transport-context-thread-"+Thread.currentThread().getId());
                boolean started_ = true;
                while (started_) {
                    final TransportEvent transportEvent = mq.peek();
                    if (transportEvent != null) {
                        final TransportMessage transportMessage = new TransportMessage(TransportMessage.TRANSPORT_MESSAGE,
                                transportEvent instanceof RemoteEvent ? 0 : Config.getConfig().LOCAL_NODE_ID, transportEvent, null);
                        mmap.put(transportMessage.getUuid(), transportMessage);

                        if (transportEvent.getChannelId() > 0) {
                            final TransportChannel channel = HeartBeatProcess.channels.get(transportEvent.getChannelId());
                            if (channel != null) {
                                channel.send(transportMessage);
                            } else {
                                logger.error("unable to find trasport channel by id="+transportEvent.getChannelId(), new RuntimeException());
                            }
                        } else {
                            if (clientChannel != null) {
                                if (!clientChannel.isStarted()) {
                                    logger.error("Client channel must be running");
                                }
                                clientChannel.send(transportMessage);
                            } else {
                                logger.error("No transport channel defined for message: "+transportMessage.getUuid());
                            }
                        }

                        mq.poll();
                        transportEvent.sent();
                    } else {
                        if (!started.get()) {
                            started_ = false;
                        }
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException ie) {
                            logger.error("exception occured", ie);
                        }
                    }
                }
            }
        });
    }

    // process incoming events
    protected void onMessage(TransportMessage transportMessage, InetAddress inetAddress) throws Exception {
        inq.put(transportMessage);
    }

    private void startIncomingMessageProcess() {
        pool2.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    while (started.get()) {
                        final TransportMessage transportMessage = inq.take();
                        onMessage(transportMessage);
                    }
                } catch (InterruptedException ie) {
                    logger.error("exception occured", ie);
                }
            }
        });
/*
        pool2.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    while (started.get()) {
                        final TransportMessage transportMessage = inq.take();
                        onMessage(transportMessage);
                    }
                } catch (InterruptedException ie) {
                            logger.error("exception occured", ie);
                }
            }
        });
*/
    }

    private void onMessage(TransportMessage transportMessage) throws InternalException {
        if (transportMessage.getType() == TransportMessage.HEARTBEAT_MESSAGE) {
            final TransportCallback transportCallback = new TransportCallback(Config.getConfig().LOCAL_NODE_ID, transportMessage.getUuid(),
                    new EventResult(TransportCallback.SUCCESS, null, 0, null, null, null));
            sendCallback(transportMessage.getSender(), new TransportMessage(TransportMessage.CALLBACK_MESSAGE, Config.getConfig().LOCAL_NODE_ID, null, transportCallback));
            logger.debug("heartbeat callback " + transportCallback.getMessageUUID() + " sent to node "+transportMessage.getSender());
        } else if (transportMessage.getType() == TransportMessage.CALLBACK_MESSAGE) {
            logger.debug("callback message received with UUID: " + transportMessage.getTransportCallback().getMessageUUID());
            processCallback(transportMessage.getTransportCallback());
        } else {
            logger.debug("transport message received with UUID: " + transportMessage.getUuid() + ", type = " + transportMessage.getTransportEvent().getClass());
            transportMessage.getTransportEvent().setCallbackNodeId(transportMessage.getSender());
            final EventResult result = transportMessage.getTransportEvent().process();
            final TransportCallback transportCallback = new TransportCallback(Config.getConfig().LOCAL_NODE_ID, transportMessage.getUuid(), result);
            sendCallback(transportMessage.getSender(), new TransportMessage(TransportMessage.CALLBACK_MESSAGE, Config.getConfig().LOCAL_NODE_ID, null, transportCallback));
            logger.debug("callback sent with UUID: " + transportMessage.getUuid() + ", type = " + transportMessage.getTransportEvent().getClass()+", destination="+transportMessage.getSender());
        }
    }

    public static TransportContext getInstance() {
        if (transportContext == null) {
            transportContext = new TransportContext();
        }
        return transportContext;
    }

    protected static TransportContext getInstance(TransportChannel clientChannel, int callbackPort) {
        if (transportContext == null) {
            transportContext = new TransportContext(clientChannel, callbackPort);
        }
        return transportContext;
    }

    public void send(TransportEvent transportEvent) {
        if (started.get()) {
            transportEvent.setLatch(new CountDownLatch(1));
            mq.offer(transportEvent);
        } else {
            transportEvent.failure(transportEvent.getChannelId(), new RuntimeException(CHANNEL_FAILURE_MESSAGE));
        }
    }

    private void sendCallback(int channelId, TransportMessage transportMessage) throws InternalException {
        final TransportChannel channel = channelId == 0 ? transportMessage.getTransportCallback().getResult().getChannel() : HeartBeatProcess.channels.get(channelId);
        if (channel != null) {
            if (transportMessage.getType() == TransportMessage.CALLBACK_MESSAGE) {
                channel.send(transportMessage);
            }
        } else {
            logger.error("Unable to find channel for channelId = "+channelId);
            for (Map.Entry<Integer, TransportChannel> entry : HeartBeatProcess.channels.entrySet()) {
                logger.error("Available channels: "+entry.getKey());
            }
            throw new InternalException();
        }
    }

    private void processCallback(TransportCallback transportCallback) {
        final TransportMessage transportMessage = mmap.get(transportCallback.getMessageUUID());
        if (transportMessage != null) {
            if (transportCallback.getResult().getResult() == TransportCallback.FAILURE) {
                transportMessage.getTransportEvent().failure(transportCallback.getNodeId(), transportCallback.getResult().getException());
            } else {
                ((TransportEventImpl) transportMessage.getTransportEvent()).setCallbackNodeId((transportCallback.getNodeId()));
                ((TransportEventImpl) transportMessage.getTransportEvent()).setCallback(transportCallback);
            }
            transportMessage.getTransportEvent().getLatch().countDown();
            if (transportMessage.getSendChannel() != null) {
                transportMessage.getSendChannel().clearSentMessage(transportMessage.getUuid());
            }
            if (transportMessage.getTransportEvent().getLatch().getCount() == 0) {
                //event processed
                mmap.remove(transportCallback.getMessageUUID());
            }
        }
        logger.debug("callback processed "+transportCallback.getMessageUUID());
    }

    protected final void storeHeartbeat(TransportMessage transportMessage) {
        if (transportMessage.getType() == TransportMessage.HEARTBEAT_MESSAGE) {
            mmap.put(transportMessage.getUuid(), transportMessage);
        }
    }

    public Integer[] getOnlineNodes() {
        return HeartBeatProcess.channels.keySet().toArray(new Integer[]{});
    }

    @SuppressWarnings("unchecked")
    public Integer[] getNodesWithLocal() {
        List<Integer> ns = Arrays.asList(HeartBeatProcess.channels.keySet().toArray(new Integer[]{}));
        List<Integer> ns_ = new ArrayList();
        ns_.addAll(ns);
        ns_.add(Config.getConfig().LOCAL_NODE_ID);
        return ns_.toArray(new Integer[]{});
    }

    @SuppressWarnings("unchecked")
    public Integer[] getOnlineNodesWithLocal() {
        List<Integer> ns_ = new ArrayList();
        ns_.add(Config.getConfig().LOCAL_NODE_ID);
        for (Map.Entry<Integer, TransportChannel> entry : HeartBeatProcess.channels.entrySet()) {
            if (entry.getValue().isConnected() && entry.getValue().isStarted()) {
                ns_.add(entry.getKey());
            }
        }
        return ns_.toArray(new Integer[]{});
    }

}
