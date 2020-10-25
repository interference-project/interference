/**
 The MIT License (MIT)

 Copyright (c) 2010-2019 head systems, ltd

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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class TransportChannel {

    private final static Logger logger = LoggerFactory.getLogger(TransportChannel.class);
    private final static int WRITE_BUFFER_SIZE = 33554432;
    private final int channelId;
    private final String channelUUID;
    private final ConcurrentLinkedQueue<TransportMessage> mq = new ConcurrentLinkedQueue<>();
    private final Map<String, TransportMessage> mmap = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<TransportMessage> cbq = new ConcurrentLinkedQueue<>();
    private final ExecutorService pool = Executors.newFixedThreadPool(1);
    private final AtomicBoolean connected =  new AtomicBoolean(false);
    private final AtomicBoolean started =  new AtomicBoolean(false);
    private final String host;
    private final int port;
    private final InetSocketAddress socketAddress;
    private final static int CONNECT_ATTEMPTS = 1;

    protected TransportChannel(String hostport) {
        this.channelId = Integer.valueOf(hostport.substring(0, hostport.indexOf(":",1)));
        this.host = hostport.substring(hostport.indexOf(":")+1, hostport.indexOf(":", hostport.indexOf(":")+1));
        this.port = Integer.valueOf(hostport.substring(hostport.indexOf(":", hostport.indexOf(":", hostport.indexOf(":")+1))+1, hostport.length()));
        this.channelUUID = UUID.randomUUID().toString();
        socketAddress = new InetSocketAddress(host, port);
    }

    protected void start(CountDownLatch latch) {
        pool.submit(new Runnable() {
            @Override
            public void run() {
                Socket sock = null;
                Thread.currentThread().setName("interference-transport-channel-"+channelId+"-thread");
                try {
                    try {
                        sock = new Socket();
                        logger.debug("try to connect to host=" + host + ":" + port);
                        sock.connect(socketAddress, 2000);
                        connected.set(true);
                        logger.info("sucessfully connected to host=" + host + ":" + port);
                        latch.countDown();
                    } catch (Exception e) {
                        latch.countDown();
                        logger.info("connection refused by host=" + host + ":" + port);
                    }
                    if (sock.isConnected()) {
                        try {
                            final ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(sock.getOutputStream(), WRITE_BUFFER_SIZE));
                            boolean running = true;
                            started.set(true);
                            Thread.currentThread().setName("transport channel thread 2");
                            while (running) {
                                final TransportMessage transportMessage = mq.peek() == null ? cbq.poll() : mq.poll();
                                try {
                                    if (transportMessage != null) {
                                        if (oos != null) {
                                            oos.writeObject(transportMessage);
                                            oos.flush();
                                            oos.reset();
                                            transportMessage.setSendChannel(TransportChannel.this);
                                            mmap.put(transportMessage.getUuid(), transportMessage);
                                            logger.debug("channel id = " + channelId + " sent " + transportMessage + " message with UUID: " + transportMessage.getUuid());
                                        } else {
                                            if (transportMessage.getType() == TransportMessage.TRANSPORT_MESSAGE || transportMessage.getType() == TransportMessage.HEARTBEAT_MESSAGE) {
                                                transportMessage.getTransportEvent().failure(channelId, new RuntimeException("Channel failure"));
                                                transportMessage.getTransportEvent().getLatch().countDown();
                                            } else if (transportMessage.getType() == TransportMessage.CALLBACK_MESSAGE) {
                                                cbq.add(transportMessage);
                                            }
                                            running = false;
                                            started.set(false);
                                            connected.set(false);
                                            cleanQueueOnFail();
                                            cleanWaitMsgsOnFail();
                                            try {
                                                sock.close();
                                            } catch (Exception e_) {
                                                e_.printStackTrace();
                                            }
                                            logger.error("channel id = " + channelId + " stopped by connection failure");
                                        }
                                    } else {
                                        try {
                                            Thread.sleep(1);
                                        } catch (InterruptedException ie) {
                                            ie.printStackTrace();
                                        }
                                    }
                                } catch (IOException e) {
                                    //e.printStackTrace();
                                    if (transportMessage != null && transportMessage.getTransportEvent() != null) {
                                        transportMessage.getTransportEvent().failure(channelId, e);
                                        transportMessage.getTransportEvent().getLatch().countDown();
                                    }
                                    running = false;
                                    started.set(false);
                                    connected.set(false);
                                    cleanQueueOnFail();
                                    cleanWaitMsgsOnFail();
                                    try {
                                        sock.close();
                                    } catch (Exception e_) {
                                        e_.printStackTrace();
                                    }
                                    logger.error("channel id = " + channelId + " stopped by connection failure");
                                }
                            }
                        } catch (IOException e) {
                            connected.set(false);
                            e.printStackTrace();
                        }
                        logger.info("channel "+channelId+" closed");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    started.set(false);
                }
            }
        });
    }

    private void cleanQueueOnFail() {
        while (mq.peek() != null) {
            final TransportMessage transportMessage = mq.poll();
            if (transportMessage.getType() == TransportMessage.TRANSPORT_MESSAGE || transportMessage.getType() == TransportMessage.HEARTBEAT_MESSAGE) {
                transportMessage.getTransportEvent().failure(channelId, new RuntimeException("Channel failure"));
                transportMessage.getTransportEvent().getLatch().countDown();
            }
        }
    }

    private void cleanWaitMsgsOnFail() {
        for (Map.Entry<String, TransportMessage> entry : mmap.entrySet()) {
            final TransportMessage transportMessage = entry.getValue();
            if (transportMessage.getType() == TransportMessage.TRANSPORT_MESSAGE || transportMessage.getType() == TransportMessage.HEARTBEAT_MESSAGE) {
                transportMessage.getTransportEvent().failure(channelId, new RuntimeException("Channel failure"));
                transportMessage.getTransportEvent().getLatch().countDown();
            }
            mmap.remove(entry.getKey());
        }
    }

    public void clearSentMessage(String uuid) {
        mmap.remove(uuid);
    }

    public String getChannelUUID() {
        return channelUUID;
    }

    public int getChannelId() {
        return channelId;
    }

    protected void send(TransportMessage transportMessage) {
        if (started.get()) {
            mq.offer(transportMessage);
        } else {
            if (transportMessage.getType() == TransportMessage.TRANSPORT_MESSAGE || transportMessage.getType() == TransportMessage.HEARTBEAT_MESSAGE) {
                transportMessage.getTransportEvent().failure(channelId, new RuntimeException("Channel failure"));
                transportMessage.getTransportEvent().getLatch().countDown();
            } else if (transportMessage.getType() == TransportMessage.CALLBACK_MESSAGE) {
                cbq.add(transportMessage);
            }
        }
    }

    public boolean isConnected() {
        return connected.get();
    }

    public boolean isStarted() {
        return started.get();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "channel id="+this.channelId+";host="+this.host+":"+this.port;
    }
}
