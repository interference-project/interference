/**
 The MIT License (MIT)

 Copyright (c) 2010-2020 head systems, ltd

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
import su.interference.persistent.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class TransportSyncTask implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(TransportSyncTask.class);
    private final static int REMOTE_SYNC_TIMEOUT = 120000;
    private final static int REMOTE_SYNC_DEFERRED_AMOUNT = 10000;
    private final ArrayList<SyncFrame> frames;
    private final Session s;

    public TransportSyncTask(ArrayList<SyncFrame> frames) {
        this.frames = frames;
        this.s = Session.getDntmSession();
    }

    //todo need refactor for MT, now pesisted frame algorithm restrict this, should running in ONE thread
    @SuppressWarnings("unchecked")
    public void run () {
        Thread.currentThread().setName("interference-transport-sync-thread");

        final SyncFrame[] sb = frames.stream().filter(b -> b.isAllowR()).collect(Collectors.toList()).toArray(new SyncFrame[]{});
        try {
            for (Map.Entry<Integer, TransportChannel> entry : HeartBeatProcess.channels.entrySet()) {
                final TransportChannel channel = entry.getValue();
                if (channel.isStarted() && channel.isConnected()) {
                    final List<FrameSync> lbs_ = Instance.getInstance().getSyncFrames(channel.getChannelId(), REMOTE_SYNC_DEFERRED_AMOUNT);
                    final String lbsUUID = lbs_.size() > 0 ? lbs_.get(0).getSyncUUID() : "NONE";
                    if (lbs_.size() > 0) {
                        logger.debug("retrieve first framesync: " + lbs_.get(0));
                    }
                    final List<FrameSync> lbs = lbs_.size() == 0 ? new ArrayList<>() : Instance.getInstance().getSyncFramesByUUID(lbs_.get(0).getSyncUUID());
                    Collections.sort(lbs);
                    logger.info(lbs.size() + " persisted sync frame(s) found (node id = " + channel.getChannelId() + ", UUID = " + lbsUUID + ")");
                    final List<SyncFrame> psb = new ArrayList<>();
                    for (FrameSync bs : lbs) {
                        if (bs.getAllocId() == CommandEvent.INITTRAN || bs.getAllocId() == CommandEvent.COMMIT || bs.getAllocId() == CommandEvent.ROLLBACK) { //command
                            final CommandEvent command = new CommandEvent((int)bs.getAllocId(), bs.getFrameId(), channel.getChannelId());
                            TransportContext.getInstance().send(command);
                            final boolean sent = command.getLatch().await(REMOTE_SYNC_TIMEOUT, TimeUnit.MILLISECONDS);
                            if (!command.isFail() && sent) {
                                logger.debug("sent persisted command sync: " + bs);
                                s.delete(bs);
                            }
                        } else {
                            FreeFrame fb = null;
                            final Frame b = Instance.getInstance().getFrameById(bs.getFrameId()).getFrame();
                            final SyncFrame sb_ = new SyncFrame(b, s, fb);
                            psb.add(sb_);
                        }
                    }
                    boolean done_ = true;
                    try {
                        if (psb.size() > 0) {
                            Collection<SyncFrame> psb_ = retrieveAdditionalFrames(psb, s);
                            final SyncFrameEvent event = new SyncFrameEvent(channel.getChannelId(), psb_.toArray(new SyncFrame[]{}));
                            TransportContext.getInstance().send(event);
                            final boolean sent = event.getLatch().await(REMOTE_SYNC_TIMEOUT, TimeUnit.MILLISECONDS);
                            if (!event.isFail() && sent) {
                                logger.info(psb.size() + "(" + psb_.size() + ") persisted sync frame(s) were sent and synced (node id = " + channel.getChannelId() + ")");
                            }
                        }
                    } catch (Exception e) {
                        done_ = false;
                        logger.error(psb.size()+" persisted sync frame(s) were not sync due to channel failure (node id = " + channel.getChannelId() + ")");
                    }
                    if (done_) {
                        for (FrameSync bs : lbs) {
                            if (bs.getAllocId() > CommandEvent.MAX_COMMAND) { //command
                                final FrameSync f_ = (FrameSync) s.find_(FrameSync.class, bs.getSyncId());
                                if (f_ != null) {
                                    logger.debug("sent persisted framesync: " + bs);
                                    s.delete(bs);
                                }
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
                        final String syncUUID = UUID.randomUUID().toString();
                        createInitTranCommands(sb, channel.getChannelId(), s);
                        for (SyncFrame b : sb) {
                            FrameSync bs = new FrameSync(b.getAllocId(), channel.getChannelId(), b.getFrameId(), syncUUID);
                            s.persist(bs);
                            logger.debug("persist framesync " + bs);
                            Metrics.get("syncQueue").put(1);
                        }
                        logger.error(sb.length+" frame(s) were not sync due to channel failure (channel id = " + channel.getChannelId() + ")");
                    }
                } else {
                    logger.info("node "+channel.getChannelId()+" unavailable");
                    final String syncUUID = UUID.randomUUID().toString();
                    createInitTranCommands(sb, channel.getChannelId(), s);
                    for (SyncFrame b : sb) {
                        FrameSync bs = new FrameSync(b.getAllocId(), channel.getChannelId(), b.getFrameId(), syncUUID);
                        s.persist(bs);
                        logger.debug("persist framesync: " + bs);
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
                FrameSync bs = new FrameSync(command, channel.getChannelId(), id, UUID.randomUUID().toString());
                s.persist(bs);
                logger.debug("persist command sync for channelId " + channel.getChannelId());
            }
        }
    }

    private void createInitTranCommands(SyncFrame[] sb, int channelId, Session s) throws Exception {
        final Map<Long, Transaction> tmap = new HashMap<>();
        final List<CommandEvent> events = new ArrayList<>();
        for (SyncFrame b : sb) {
            for (Map.Entry<Long, Transaction> entry : b.getRtran().entrySet()) {
                tmap.put(entry.getKey(), entry.getValue());
            }
        }
        for (Map.Entry<Long, Transaction> entry : tmap.entrySet()) {
            final FrameSync fs = new FrameSync(CommandEvent.INITTRAN, channelId, entry.getKey(), UUID.randomUUID().toString());
            s.persist(fs);
        }
    }

    private Collection<SyncFrame> retrieveAdditionalFrames(List<SyncFrame> sb, Session s) throws Exception {
        Map<Long, SyncFrame> add = new HashMap<>();
        for (SyncFrame f : sb) {
            add.put(f.getAllocId(), f);
        }
        FreeFrame fb = null;
        for (SyncFrame f : sb) {
            if (add.get(f.getNextId()) == null) {
                final FrameData nextF = f.getNextId() == 0 ? null : Instance.getInstance().getFrameByAllocId(f.getNextId());
                if (nextF != null) {
                    add.put(f.getNextId(), new SyncFrame(nextF.getFrame(), s, fb, false));
                }
            }
            if (add.get(f.getParentId()) == null) {
                final FrameData parentF = f.getParentId() == 0 ? null : Instance.getInstance().getFrameByAllocId(f.getParentId());
                if (parentF != null) {
                    add.put(f.getParentId(), new SyncFrame(parentF.getFrame(), s, fb, false));
                }
            }
            if (add.get(f.getLcId()) == null) {
                final FrameData lcF = f.getLcId() == 0 ? null : Instance.getInstance().getFrameByAllocId(f.getLcId());
                if (lcF != null) {
                    add.put(f.getLcId(), new SyncFrame(lcF.getFrame(), s, fb, false));
                }
            }
            if (f.getImap() != null) {
                for (Map.Entry<Long, Long> entry : f.getImap().entrySet()) {
                    if (add.get(entry.getValue()) == null) {
                        final FrameData allocF = entry.getValue() == 0 ? null : Instance.getInstance().getFrameByAllocId(entry.getValue());
                        if (allocF != null) {
                            add.put(entry.getValue(), new SyncFrame(allocF.getFrame(), s, fb, false));
                        }
                    }
                }
            }
        }
        return add.values();
    }

}
