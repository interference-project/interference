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
import su.interference.core.Config;
import su.interference.core.Instance;
import su.interference.core.ManagedProcess;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public final class HeartBeatProcess implements Runnable, ManagedProcess {

    private volatile boolean f = true;
    private volatile CountDownLatch latch;
    private final static Logger logger = LoggerFactory.getLogger(HeartBeatProcess.class);

    protected final static Map<Integer, TransportChannel> channels = new HashMap<>();

    static {
        for (int i = 0; i < Config.getConfig().CLUSTER_NODES.length; i++) {
            final TransportChannel channel = new TransportChannel(Config.getConfig().CLUSTER_NODES[i]);
            channels.put(channel.getChannelId(), channel);
        }
    }

    public void run () {
        Thread.currentThread().setName("interference-heartbeat-thread-"+Thread.currentThread().getId());
        if (Instance.getInstance().getClusterState() == Instance.CLUSTER_STATE_DOWN) {
            f = false;
        }

        while (f) {
            latch = new CountDownLatch(1);
            for (Map.Entry<Integer, TransportChannel> entry: channels.entrySet()) {
                if (!entry.getValue().isStarted()) {
                    CountDownLatch latch_ = new CountDownLatch(1);
                    entry.getValue().start(latch_);
                }
                if (entry.getValue().isConnected()) {
                    logger.info(entry.getValue().toString()+" connected");
                    try {
                        final int callbackNodeId = heartbeat(entry.getValue());
                        if (callbackNodeId > 0) {
                            logger.debug("heartbeat successful");
                        } else {
                            logger.debug("heartbeat failed");
                        }
                    } catch (Exception e) {
                        logger.error("exception occured during heartbeat process", e);
                    }
                } else {
                    logger.info(entry.getValue()+" not connected");
                }
            }

            logger.debug("channel start iteration complete");

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                f = false;
            }
            latch.countDown();
        }

    }

    public void stop() throws InterruptedException{
        f = false;
        if (latch != null) {
            latch.await();
        }
    }

    private int heartbeat (final TransportChannel transportChannel) {
        final TransportEvent event = new TransportEventImpl(transportChannel.getChannelId());
        final TransportMessage transportMessage = new TransportMessage(TransportMessage.HEARTBEAT_MESSAGE, Config.getConfig().LOCAL_NODE_ID, event, null);
        try {
            event.setLatch(new CountDownLatch(1));
            transportChannel.send(transportMessage);
            TransportContext.getInstance().storeHeartbeat(transportMessage);
            final boolean await = event.getLatch().await(1000, TimeUnit.MILLISECONDS);
            logger.debug("heartbeat callback await returns "+await);
            if (await) {
                return ((TransportEventImpl) event).getCallbackNodeId();
            } else {
                return 0;
            }
        } catch (InterruptedException e) {
            logger.error("InterruptedException occured during heartbeat request");
            return 0;
        }
    }

}
