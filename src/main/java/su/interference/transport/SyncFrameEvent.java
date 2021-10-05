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
import su.interference.core.*;
import su.interference.exception.InternalException;
import su.interference.metrics.Metrics;
import su.interference.persistent.*;
import su.interference.sql.ContainerFrame;
import su.interference.sql.FrameApi;
import su.interference.sql.SQLCursor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SyncFrameEvent extends TransportEventImpl {

    private final static long serialVersionUID = 436398796234081233L;
    private final static Logger logger = LoggerFactory.getLogger(SyncFrameEvent.class);
    private SyncFrame[] sb;

    public SyncFrameEvent(int channelId, SyncFrame[] sb) throws InternalException {
        super(channelId);
        this.sb = sb;
    }

    @Override
    public EventResult process() {
        try {
            rframe2(this.sb);
        } catch (Exception e) {
            logger.error("exception occured during sync event process", e);
            return new EventResult(TransportCallback.FAILURE, null, 0, null, e, null);
        }
        return new EventResult(TransportCallback.SUCCESS, null, 0, null, null, null);
    }

    public synchronized int rframe2(SyncFrame[] sb) throws Exception {
        Session s = Session.getDntmSession();
        Map<Long, Long> hmap = new HashMap<Long, Long>();
        Map<Long, Long> hmap2 = new HashMap<Long, Long>();
        Metrics.get("syncFrameEvent").start();
        final LLT llt = null;

        for (SyncFrame b : sb) {
            if (b.isAllowR()) {
                updateTransactions(b.getRtran(), s);
                FrameData bd = Instance.getInstance().getFrameByAllocId(b.getAllocId());
                final Table t = Instance.getInstance().getTableByName(b.getClassName());
                if (t == null || b.isFree()) {
                    final FreeFrame fb = new FreeFrame(0, bd.getFrameId(), bd.getSize());
                    s.persist(fb, llt);
                    b.setDf(Instance.getInstance().getDataFileById(bd.getFile()));
                    s.delete(bd);
                } else {
                    if (bd == null) {
                        final int allocFileId = (int) b.getAllocFile();
                        final int allocOrder = (allocFileId % Storage.MAX_NODES) % Config.getConfig().FILES_AMOUNT;
                        ArrayList<DataFile> dfs = Instance.getInstance().getDataFilesByType(b.getFileType());
                        for (DataFile f : dfs) {
                            final int order = (f.getFileId() % Storage.MAX_NODES) % Config.getConfig().FILES_AMOUNT;
                            if (order == allocOrder) {
                                final LLT llt_ = LLT.getLLT(); //df access reordering prevent deadlock
                                bd = t.createNewFrame(null, null, f.getFileId(), b.getFrameType(), b.getAllocId(), false, false, true, s, llt_);
                                llt_.commit();
                                bd.setFrame(null);
                                b.setDf(f);
                            }
                        }
                        logger.debug("create new frame allocId " + b.getAllocId() + " ptr " + bd.getFrameId());
                    } else {
                        final Table t_ = Instance.getInstance().getTableById(bd.getObjectId());
                        if (t.getName().equals(t_.getName())) {
                            b.setDf(Instance.getInstance().getDataFileById(bd.getFile()));
                            logger.debug("rframe bd " + bd.getFrameId() + " found with allocId=" + b.getAllocId());
                        } else {
                            final FrameData bd_ = new FrameData(bd, t);
                            s.delete(bd);
                            s.persist(bd_);
                            b.setDf(Instance.getInstance().getDataFileById(bd_.getFile()));
                        }
                    }
                }

                b.setBd(bd);
                hmap.put(b.getAllocId(), bd.getFrameId());
                hmap2.put(b.getFrameId(), bd.getFrameId());
            }
        }

        final Map<Long, Long> umap2 = new ConcurrentHashMap<>();

        for (SyncFrame b : sb) {
            try {
                if (b.isAllowR() && b.isProc()) {
                    final Table t = Instance.getInstance().getTableByName(b.getClassName());
                    final long prevId_ = b.getPrevId() == 0 ? 0 : hmap.get(b.getPrevId()) != null ? hmap.get(b.getPrevId()) : Instance.getInstance().getFrameByAllocId(b.getPrevId()).getFrameId();
                    final long nextId_ = b.getNextId() == 0 ? 0 : hmap.get(b.getNextId()) != null ? hmap.get(b.getNextId()) : Instance.getInstance().getFrameByAllocId(b.getNextId()).getFrameId();
                    final long prevF = prevId_ % 4096;
                    final long prevB = prevId_ - prevF;
                    final long nextF = nextId_ % 4096;
                    final long nextB = nextId_ - nextF;

                    if (b.getBd() == null) {
                        throw new InternalException();
                    } else {
                        if (b.getFrameType() == 99) {
                            Frame frame = new DataFrame(b.getBytes(), b.getBd().getFile(), b.getBd().getPtr(), b.getImap(), hmap, t, s);
                            frame.setRes01((int)prevF);
                            frame.setRes02((int)nextF);
                            frame.setRes06(prevB);
                            frame.setRes07(nextB);
                            frame.setFrameData(b.getBd());
                            b.getBd().setFrame(frame);
                            final LLT llt_ = LLT.getLLT(); //df access reordering prevent deadlock
                            b.getDf().writeFrame(b.getBd(), b.getBd().getPtr(), frame.getFrame(), llt_, s);
                            llt_.commit();
                            umap2.put(b.getBd().getFrameId(), b.getBd().getAllocId());
                            logger.debug("write undo frame with allocId "+b.getAllocId()+" ptr "+b.getBd().getFrameId());
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("exception occured during sync event process", e);
            }
        }

        // check and complete umap
        // prevent unconsistency of undo data due to the fact that
        // the one of undo frames for concrete data frame may be out
        // of current sync pack
        final Map<Long, Long> uframes = new HashMap<>();
        for (SyncFrame b : sb) {
            if (b.isProc()) {
                for (Map.Entry<Long, List<Long>> entry : b.getUFrames().entrySet()) {
                    final List<Long> ulist = new ArrayList<>();
                    final Transaction tran = Instance.getInstance().getTransactionById(entry.getKey());
                    for (Long uallocId : entry.getValue()) {
                        final long uframeId = Instance.getInstance().getFrameByAllocId(uallocId).getFrameId();
                        uframes.put(uframeId, uallocId);
                        ulist.add(uframeId);
                        tran.storeRFrame(uframeId, b.getBd().getFrameId(), b.getBd().getObjectId(), s);
                    }
                    b.getBd().updateTCounter(entry.getKey(), ulist);
                }
            }
        }
        for (Map.Entry<Long, Long> entry : umap2.entrySet()) {
            final Long allocId = uframes.get(entry.getKey());
            if (allocId == null) {
                throw new RuntimeException("internal error: synced undo frame not found in uframes map");
            }
        }

        for (SyncFrame b : sb) {
            try {
                if (b.isAllowR() && b.isProc()) {
                    final Table t = Instance.getInstance().getTableByName(b.getClassName());
                    final long prevId_ = b.getPrevId() == 0 ? 0 : hmap.get(b.getPrevId()) != null ? hmap.get(b.getPrevId()) : Instance.getInstance().getFrameByAllocId(b.getPrevId()).getFrameId();
                    final long nextId_ = b.getNextId() == 0 ? 0 : hmap.get(b.getNextId()) != null ? hmap.get(b.getNextId()) : Instance.getInstance().getFrameByAllocId(b.getNextId()).getFrameId();
                    final long prevF = prevId_ % 4096;
                    final long prevB = prevId_ - prevF;
                    final long nextF = nextId_ % 4096;
                    final long nextB = nextId_ - nextF;

                    if (b.getBd() == null) {
                        throw new InternalException();
                    } else {
                        if (b.getFrameType() == 0) {
                            Frame frame = new DataFrame(b.getBytes(), b.getBd().getFile(), b.getBd().getPtr(), t);
                            frame.setRes01((int)prevF);
                            frame.setRes02((int)nextF);
                            frame.setRes06(prevB);
                            frame.setRes07(nextB);
                            frame.setFrameData(b.getBd());
                            b.getBd().setFrame(frame);
                            final LLT llt_ = LLT.getLLT(); //df access reordering prevent deadlock
                            b.getDf().writeFrame(b.getBd(), b.getBd().getPtr(), frame.getFrame(), llt_, s);
                            llt_.commit();
                            logger.debug("write data frame with allocId "+b.getAllocId()+" ptr "+b.getBd().getFrameId()+" size "+frame.getChunks().size());
                            b.getBd().setFrame(null);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("exception occured during sync event process", e);
            }
        }

        final Map<Integer, List<SyncFrame>> storemap = new HashMap<>();

        for (SyncFrame b : sb) {
            try {
                if (b.isAllowR() && b.isProc()) {
                    final Table t = Instance.getInstance().getTableByName(b.getClassName());
                    final long parentId_ = b.getParentId() == 0 ? 0 : hmap.get(b.getParentId()) != null ? hmap.get(b.getParentId()) : Instance.getInstance().getFrameByAllocId(b.getParentId()).getFrameId();
                    final long lcId_ = b.getLcId() == 0 ? 0 : hmap.get(b.getLcId()) != null ? hmap.get(b.getLcId()) : Instance.getInstance().getFrameByAllocId(b.getLcId()).getFrameId();
                    final long parentF = parentId_ % 4096;
                    final long parentB = parentId_ - parentF;
                    final long lcF = lcId_ % 4096;
                    final long lcB = lcId_ - lcF;

                    if (b.getBd() == null) {
                        throw new InternalException();
                    } else {
                        if (b.getFrameType() == 1 || b.getFrameType() == 2) {
                            IndexFrame frame = new IndexFrame(b.getBytes(), b.getBd().getFile(), b.getBd().getPtr(), b.getImap(), hmap, t);
                            frame.setRes04((int)parentF);
                            frame.setRes05((int)lcF);
                            frame.setRes06(parentB);
                            frame.setRes07(lcB);
                            b.setRFrame(frame);
                            if (storemap.get(t.getObjectId()) == null) {
                                storemap.put(t.getObjectId(), new ArrayList<>());
                            }
                            storemap.get(t.getObjectId()).add(b);
                            logger.debug("write index frame with allocId "+b.getAllocId()+" ptr "+b.getBd().getFrameId());
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("exception occured during sync event process", e);
            }
        }

        for (Map.Entry<Integer, List<SyncFrame>> entry : storemap.entrySet()) {
            final Table t = Instance.getInstance().getTableById(entry.getKey());
            if (this.getCallbackNodeId() == 0) {
                throw new RuntimeException(TransportContext.WRONG_CALLBACK_NODE_MESSAGE);
            }
            final LLT llt_ = LLT.getLLT();
            t.storeFrames(entry.getValue(), this.getCallbackNodeId(), llt_, s);
            llt_.commit();
        }

        final Map<Integer, List<FrameApi>> frames_ = new HashMap<>();
        for (SyncFrame f : sb) {
            if (f.isAllowR() && f.isProc()) {
                f.getBd().setSynced();

                final Table t = Instance.getInstance().getTableByName(f.getClassName());
                if (frames_.get(t.getObjectId()) == null) {
                    frames_.put(t.getObjectId(), new ArrayList<>());
                }
                frames_.get(t.getObjectId()).add(f.getBd());
            }
        }
        for (Map.Entry<Integer, List<FrameApi>> entry: frames_.entrySet()) {
            SQLCursor.addStreamFrame(new ContainerFrame(entry.getKey(), entry.getValue()));
        }

        Metrics.get("syncFrameEvent").stop();
        logger.info(sb.length + " frame(s) were received and synced");

        return 0;
    }

    private void updateTransactions(Map<Long, Transaction> rtran, Session s) {
        for (Map.Entry<Long, Transaction> entry : rtran.entrySet()) {
            final Transaction tran = Instance.getInstance().getTransactionById(entry.getKey());
            if (tran == null) {
                final Transaction tran_ = new Transaction(entry.getValue());
                try {
                    s.persist(tran_);
                } catch (Exception e) {
                    logger.error("unable to persist remote transaction", e);
                }
            }
        }
    }

}
