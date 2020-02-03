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
        //ArrayList<TransFrame> tframes = new ArrayList<>();
        for (SyncFrame b : sb) {
            if (b.isAllowR()) {
                updateTransactions(b.getRtran(), s);
                FrameData bd = Instance.getInstance().getFrameByAllocId(b.getAllocId());
                if (bd == null) {
                    final Table t = Instance.getInstance().getTableByName(b.getClassName());
                    final int allocFileId = (int) b.getAllocId() % 4096;
                    final int allocOrder = (allocFileId % Storage.MAX_NODES) % Config.getConfig().FILES_AMOUNT;
                    ArrayList<DataFile> dfs = Instance.getInstance().getDataFilesByType(b.getFileType());
                    for (DataFile f : dfs) {
                        final int order = (f.getFileId() % Storage.MAX_NODES) % Config.getConfig().FILES_AMOUNT;
                        if (order == allocOrder) {
                            //final LLT llt = LLT.getLLT();
                            bd = t.createNewFrame(null, f.getFileId(), b.getFrameType(), b.getAllocId(), b.isStarted(), false, true, s, null);
                            //llt.commit();
                            //bd.setAllocId(b.getAllocId());
                            bd.setFrame(null);
                            b.setDf(f);
                        }
                    }
                    logger.debug("create replicated frame with allocId "+b.getAllocId()+" ptr "+bd.getPtr());
                } else {
                    if (b.getObjectId() == bd.getObjectId()) {
                        bd.setStarted(b.isStarted()?1:0);
                        s.persist(bd);
                        b.setDf(Instance.getInstance().getDataFileById(bd.getFile()));
                        logger.debug("rframe bd found with allocId=" + b.getAllocId());
                    } else {
                        if (b.getObjectId() == 0) {
                            final FreeFrame fb = new FreeFrame(0, bd.getFrameId(), bd.getSize());
                            s.persist(fb);
                            b.setDf(Instance.getInstance().getDataFileById(bd.getFile()));
                            s.delete(bd);
                        } else {
                            bd.setStarted(b.isStarted()?1:0);
                            bd.setObjectId(b.getObjectId());
                            s.persist(bd);
                            b.setDf(Instance.getInstance().getDataFileById(bd.getFile()));
                        }
                    }
                }

                b.setBd(bd);
                hmap.put(b.getAllocId(), bd.getFrameId());
                hmap2.put(b.getFrameId(), bd.getFrameId());
                //tframes.addAll(b.getTframes());
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
                            b.getDf().writeFrame(b.getBd().getPtr(), frame.getFrame());
                            b.getBd().setFrame(null);
                            logger.debug("write undo frame with allocId "+b.getAllocId()+" ptr "+b.getBd().getPtr());
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
                            b.getDf().writeFrame(b.getBd().getPtr(), frame.getFrame());
                            b.getBd().setFrame(frame);
                            logger.debug("write data frame with allocId "+b.getAllocId()+" ptr "+b.getBd().getPtr());
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            //b.getBd().setFrame(null);
        }

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
                            IndexFrame frame = new IndexFrame(b.getBytes(), b.getBd().getFile(), b.getBd().getPtr(), b.getImap(), hmap, t);
                            frame.setRes04(parentF);
                            frame.setRes05(lcF);
                            frame.setRes06(parentB);
                            frame.setRes07(lcB);
                            b.getDf().writeFrame(b.getBd().getPtr(), frame.getFrame());
                            b.getBd().setFrame(frame);
                            logger.debug("write index frame with allocId "+b.getAllocId()+" ptr "+b.getBd().getPtr());
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            //b.getBd().setFrame(null);
        }

        final Map<Integer, List<FrameApi>> frames_ = new HashMap<>();
        for (SyncFrame f : sb) {
            if (frames_.get(f.getObjectId()) == null) {
                frames_.put(f.getObjectId(), new ArrayList<>());
            }
            frames_.get(f.getObjectId()).add(f.getBd());
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

    private void updateTransFrames(ArrayList<TransFrame> tframes, HashMap<Long, Long> hmap2, Session s) throws Exception {
        for (TransFrame tb : tframes) {
            final long cframeid = hmap2.get(tb.getCframeId());
            if (cframeid == 0) {
                throw new InternalException();
            }
            final long uframeid = tb.getUframeId() == 0 ? 0 : hmap2.get(tb.getUframeId());
            if (tb.getUframeId() > 0) {
                if (uframeid == 0) {
                    throw new InternalException();
                }
            }
            tb.setCframeId(cframeid);
            tb.setUframeId(uframeid);
            s.persist(tb);
        }
    }

}
