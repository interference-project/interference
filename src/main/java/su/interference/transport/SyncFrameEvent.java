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
            e.printStackTrace();
            return new EventResult(TransportCallback.FAILURE, 0, null, e);
        }
        return new EventResult(TransportCallback.SUCCESS, 0, null, null);
    }

    public synchronized int rframe2(SyncFrame[] sb) throws Exception {
        Session s = Session.getDntmSession();
        HashMap<Long, Long> hmap = new HashMap<Long, Long>();
        HashMap<Long, Long> hmap2 = new HashMap<Long, Long>();
//        LLT llt = LLT.getLLT();
        final LLT llt = null;
        for (SyncFrame b : sb) {
            if (b.isAllowR()) {
                updateTransactions(b.getRtran(), s);
                FrameData bd = Instance.getInstance().getFrameByAllocId(b.getAllocId());
                final Table t = Instance.getInstance().getTableByName(b.getClassName());
                if (t == null) {
                    final FreeFrame fb = new FreeFrame(0, bd.getFrameId(), bd.getSize());
                    s.persist(fb, llt);
                    b.setDf(Instance.getInstance().getDataFileById(bd.getFile()));
                    s.delete(bd);
                } else {
                    if (bd == null) {
                        final int allocFileId = (int) b.getAllocId() % 4096;
                        final int allocOrder = (allocFileId % Storage.MAX_NODES) % Config.getConfig().FILES_AMOUNT;
                        ArrayList<DataFile> dfs = Instance.getInstance().getDataFilesByType(b.getFileType());
                        for (DataFile f : dfs) {
                            final int order = (f.getFileId() % Storage.MAX_NODES) % Config.getConfig().FILES_AMOUNT;
                            if (order == allocOrder) {
                                final LLT llt_ = LLT.getLLT(); //df access reordering prevent deadlock
                                bd = t.createNewFrame(null, f.getFileId(), b.getFrameType(), b.getAllocId(), false, false, true, s, llt_);
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
        //updateTransFrames(tframes, hmap2, s);

        final Map<Long, List<Chunk>> umap = new ConcurrentHashMap<>();

        for (SyncFrame b : sb) {
            try {
                if (b.isAllowR()) {
                    final Table t = Instance.getInstance().getTableByName(b.getClassName());
                    final long prevId_ = b.getPrevId() == 0 ? 0 : hmap.get(b.getPrevId()) != null ? hmap.get(b.getPrevId()) : Instance.getInstance().getFrameByAllocId(b.getPrevId()).getFrameId();
                    final long nextId_ = b.getNextId() == 0 ? 0 : hmap.get(b.getNextId()) != null ? hmap.get(b.getNextId()) : Instance.getInstance().getFrameByAllocId(b.getNextId()).getFrameId();
                    final int prevF = (int) prevId_ % 4096;
                    final long prevB = prevId_ - prevId_ % 4096;
                    final int nextF = (int) nextId_ % 4096;
                    final long nextB = nextId_ - nextId_ % 4096;

                    if (b.getBd() == null) {
                        throw new InternalException();
                    } else {
                        if (b.getFrameType() == 99) {
                            Frame frame = new DataFrame(b.getBytes(), b.getBd().getFile(), b.getBd().getPtr(), b.getImap(), hmap, umap, t, s);
                            frame.setRes01(prevF);
                            frame.setRes02(nextF);
                            frame.setRes06(prevB);
                            frame.setRes07(nextB);
                            frame.setFrameData(b.getBd());
                            b.getBd().setFrame(frame);
                            final LLT llt_ = LLT.getLLT(); //df access reordering prevent deadlock
                            b.getDf().writeFrame(b.getBd(), b.getBd().getPtr(), frame.getFrame(), llt_, s);
                            llt_.commit();
//                            b.getBd().setFrame(null);
                            logger.debug("write undo frame with allocId "+b.getAllocId()+" ptr "+b.getBd().getFrameId());
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (SyncFrame b : sb) {
            try {
                if (b.isAllowR()) {
                    final Table t = Instance.getInstance().getTableByName(b.getClassName());
                    final long prevId_ = b.getPrevId() == 0 ? 0 : hmap.get(b.getPrevId()) != null ? hmap.get(b.getPrevId()) : Instance.getInstance().getFrameByAllocId(b.getPrevId()).getFrameId();
                    final long nextId_ = b.getNextId() == 0 ? 0 : hmap.get(b.getNextId()) != null ? hmap.get(b.getNextId()) : Instance.getInstance().getFrameByAllocId(b.getNextId()).getFrameId();
                    final int prevF = (int) prevId_ % 4096;
                    final long prevB = prevId_ - prevId_ % 4096;
                    final int nextF = (int) nextId_ % 4096;
                    final long nextB = nextId_ - nextId_ % 4096;

                    if (b.getBd() == null) {
                        throw new InternalException();
                    } else {
                        if (b.getFrameType() == 0) {
                            Frame frame = new DataFrame(b.getBytes(), b.getBd().getFile(), b.getBd().getPtr(), umap, t);
                            frame.setRes01(prevF);
                            frame.setRes02(nextF);
                            frame.setRes06(prevB);
                            frame.setRes07(nextB);
                            frame.setFrameData(b.getBd());
                            b.getBd().setFrame(frame);
                            final LLT llt_ = LLT.getLLT(); //df access reordering prevent deadlock
                            b.getDf().writeFrame(b.getBd(), b.getBd().getPtr(), frame.getFrame(), llt_, s);
                            llt_.commit();
//                            logger.debug("write data frame with allocId "+b.getAllocId()+" ptr "+b.getBd().getFrameId()+" size "+frame.getChunks().size());
                            logger.debug("allocId "+b.getAllocId()+" ptr "+b.getBd().getFrameId());
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            //b.getBd().setFrame(null);
        }

        final Map<Integer, List<SyncFrame>> storemap = new HashMap<>();

        for (SyncFrame b : sb) {
            try {
                if (b.isAllowR()) {
                    final Table t = Instance.getInstance().getTableByName(b.getClassName());
                    final long parentId_ = b.getParentId() == 0 ? 0 : hmap.get(b.getParentId()) != null ? hmap.get(b.getParentId()) : Instance.getInstance().getFrameByAllocId(b.getParentId()).getFrameId();
                    final long lcId_ = b.getLcId() == 0 ? 0 : hmap.get(b.getLcId()) != null ? hmap.get(b.getLcId()) : Instance.getInstance().getFrameByAllocId(b.getLcId()).getFrameId();
                    final int parentF = (int) parentId_ % 4096;
                    final long parentB = parentId_ - parentId_ % 4096;
                    final int lcF = (int) lcId_ % 4096;
                    final long lcB = lcId_ - lcId_ % 4096;

                    if (b.getBd() == null) {
                        throw new InternalException();
                    } else {
                        if (b.getFrameType() == 1 || b.getFrameType() == 2) {
                            IndexFrame frame = new IndexFrame(b.getBytes(), b.getBd().getFile(), b.getBd().getPtr(), b.getImap(), hmap, umap, t);
                            frame.setRes04(parentF);
                            frame.setRes05(lcF);
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
                e.printStackTrace();
            }
            //b.getBd().setFrame(null);
        }

        for (Map.Entry<Integer, List<SyncFrame>> entry : storemap.entrySet()) {
            final Table t = Instance.getInstance().getTableById(entry.getKey());
            if (this.getCallbackNodeId() == 0) {
                throw new RuntimeException("wrong callback node id");
            }
            final LLT llt_ = LLT.getLLT();
            t.storeFrames(entry.getValue(), this.getCallbackNodeId(), llt_, s);
            llt_.commit();
        }

//        llt.commit();

        final Map<Integer, List<FrameApi>> frames_ = new HashMap<>();
        for (SyncFrame f : sb) {
            if (f.isAllowR()) {
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

        logger.info(sb.length + " frame(s) were received and synced");

        return 0;

    }

    private void updateTransactions(HashMap<Long, Transaction> rtran, Session s) {
        for (Map.Entry<Long, Transaction> entry : rtran.entrySet()) {
            Transaction tran = Instance.getInstance().getTransactionById(entry.getKey());
            if (tran==null) {
                try {
                    s.persist(entry.getValue());
                } catch(Exception e) {
                    logger.error("unable to persist remote transaction", e);
                }
            }
        }
    }

}
