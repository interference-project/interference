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
import su.interference.core.*;
import su.interference.exception.InternalException;
import su.interference.metrics.Metrics;
import su.interference.persistent.FrameSync;
import su.interference.persistent.FreeFrame;
import su.interference.persistent.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class TransportSyncTask implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(TransportSyncTask.class);
    private final static int REMOTE_SYNC_TIMEOUT = 120000;
    private final ArrayList<SyncFrame> frames;
    private final Session s;

    public TransportSyncTask(ArrayList<SyncFrame> frames) {
        this.frames = frames;
        this.s = Session.getDntmSession();
    }

    //todo need refactor for MT, now pesisted frame algorithm restrict this, should running in ONE thread
    public void run () {
        Thread.currentThread().setName("interference-transport-sync-thread");

        final SyncFrame[] sb = frames.stream().filter(b -> b.isAllowR()).collect(Collectors.toList()).toArray(new SyncFrame[]{});
        try {
            for (Map.Entry<Integer, TransportChannel> entry : HeartBeatProcess.channels.entrySet()) {
                final TransportChannel channel = entry.getValue();
                if (channel.isStarted() && channel.isConnected()) {
                    final List<FrameSync> lbs = Instance.getInstance().getSyncFrames(channel.getChannelId());
                    logger.info(lbs.size() + " persisted sync frame(s) found (node id = " + channel.getChannelId() + ")");
                    final List<SyncFrame> psb_ = new ArrayList<>();
                    for (FrameSync bs : lbs) {
                        if (bs.getAllocId() == CommandEvent.COMMIT || bs.getAllocId() == CommandEvent.ROLLBACK) { //command
                            final CommandEvent command = new CommandEvent((int)bs.getAllocId(), bs.getFrameId(), channel.getChannelId());
                            TransportContext.getInstance().send(command);
                            final boolean sent = command.getLatch().await(REMOTE_SYNC_TIMEOUT, TimeUnit.MILLISECONDS);
                            if (!command.isFail() && sent) {
                                s.delete(bs);
                                logger.info("sent persisted command sync to node " + channel.getChannelId());
                            }
                        } else {
                            FreeFrame fb = null;
                            final Frame b = Instance.getInstance().getFrameById(bs.getFrameId()).getFrame();
                            final SyncFrame sb_ = new SyncFrame(b, s, fb);
                            psb_.add(sb_);
                        }
                    }
                    boolean done_ = true;
                    final SyncFrame[] psb = psb_.stream().filter(b -> b.isAllowR()).collect(Collectors.toList()).toArray(new SyncFrame[]{});
                    try {
                        if (psb.length > 0) {
                            final SyncFrameEvent event = new SyncFrameEvent(channel.getChannelId(), psb);
                            TransportContext.getInstance().send(event);
                            final boolean sent = event.getLatch().await(REMOTE_SYNC_TIMEOUT, TimeUnit.MILLISECONDS);
                            if (!event.isFail() && sent) {
                                logger.info(psb.length + " persisted sync frame(s) were sent and synced (node id = " + channel.getChannelId() + ")");
                            }
                        }
                    } catch (Exception e) {
                        done_ = false;
                        logger.error(psb.length+" persisted sync frame(s) were not sync due to channel failure (node id = " + channel.getChannelId() + ")");
                    }
                    if (done_) {
                        for (FrameSync bs : lbs) {
                            if (bs.getAllocId() > CommandEvent.MAX_COMMAND) { //command
                                s.delete(bs);
                                logger.info("sent persisted framesync with allocId = " + bs.getAllocId() + " to node " + channel.getChannelId());
                            }
                        }
                    }
                    try {
                        if (sb.length > 0) {
                            final SyncFrameEvent event = new SyncFrameEvent(channel.getChannelId(), sb);
                            TransportContext.getInstance().send(event);
                            final boolean sent = event.getLatch().await(REMOTE_SYNC_TIMEOUT, TimeUnit.MILLISECONDS);
                            if (event.isFail() || !sent) {
                                if (event.getProcessException() != null) {
                                    event.getProcessException().printStackTrace();
                                }
                                throw new InternalException();
                            }
                            logger.info(sb.length + " frame(s) were sent and synced (node id = " + channel.getChannelId() + ")");
                        }
                    } catch (Exception e) {
                        for (SyncFrame b : sb) {
                            final ArrayList<FrameSync> sbs = Instance.getInstance().getSyncFramesById(b.getFrameId());
                            boolean persist = true;
                            for (FrameSync bs_ : sbs) {
                                if (bs_.getNodeId() == channel.getChannelId()) {
                                    persist = false;
                                    break;
                                }
                            }
                            if (persist) {
                                FrameSync bs = new FrameSync(b.getAllocId(), channel.getChannelId(), b.getFrameId());
                                s.persist(bs);
                                logger.debug("persist framesync with allocId = "+b.getAllocId()+" for channel " + channel.getChannelId());
                            }
                            Metrics.get("syncQueue").put(1);
                        }
                        logger.error(sb.length+" frame(s) were not sync due to channel failure (channel id = " + channel.getChannelId() + ")");
                    }
                } else {
                    logger.info("node "+channel.getChannelId()+" unavailable");
                    for (SyncFrame b : sb) {
                        final ArrayList<FrameSync> sbs = Instance.getInstance().getSyncFramesById(b.getFrameId());
                        boolean persist = true;
                        for (FrameSync bs_ : sbs) {
                            if (bs_.getNodeId() == channel.getChannelId()) {
                                persist = false;
                                break;
                            }
                        }
                        if (persist) {
                            FrameSync bs = new FrameSync(b.getAllocId(), channel.getChannelId(), b.getFrameId());
                            s.persist(bs);
                            logger.debug("persist framesync with allocId = "+b.getAllocId()+" for channel " + channel.getChannelId());
                        }
                    }
                    logger.info(sb.length+" frame(s) were not sync due to node unavailable (channel id = " + channel.getChannelId() + ")");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static synchronized void sendBroadcastCommand(int command, long id, Session s) throws Exception {
        for (Map.Entry<Integer, TransportChannel> entry : HeartBeatProcess.channels.entrySet()) {
            final TransportChannel channel = entry.getValue();
            final CommandEvent event = new CommandEvent(command, id, channel.getChannelId());
            TransportContext.getInstance().send(event);
            event.getLatch().await();
            if (event.isFail()) {
                FrameSync bs = new FrameSync(command, channel.getChannelId(), id);
                s.persist(bs);
                logger.debug("persist command sync for channelId " + channel.getChannelId());
            }
        }
    }

}
