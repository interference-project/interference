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

package su.interference.persistent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.*;
import su.interference.metrics.Metrics;
import su.interference.mgmt.MgmtColumn;
import su.interference.exception.InternalException;
import su.interference.sql.SQLJoin;
import su.interference.transport.*;

import javax.persistence.*;
import java.io.Serializable;
import java.util.*;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
@DisableSync
public class Transaction implements Serializable {
    @Transient
    public static final int TRAN_READ_COMMITTED = 0;
    @Transient
    public static final int TRAN_SERIALIZABLE   = 1;
    @Transient
    public static final int TRAN_THR            = 9;

    @Id
    @Column
    @IndexColumn
    @GeneratedValue
    @DistributedId
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private long transId;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private long sid;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private long timeStamp;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int  transType; // 0 - READ COMMITTED, 1 - SERIALIZABLE, 9 - THR
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private long mTran;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private long cid;

    @Transient
    private final List<TransFrame> tframes = new CopyOnWriteArrayList<>();
    @Transient
    private final transient WaitFrame[] lbs;
    @Transient
    private final AtomicInteger avframeStart = new AtomicInteger(0);
    @Transient
    private transient SQLJoin join;
    @Transient
    boolean started = false;
    @Transient
    public static final int CLASS_ID = 7;
    @Transient
    private final static long serialVersionUID = 123214870766599481L;

    public static int getCLASS_ID() {
        return CLASS_ID;
    }

    @Transient
    private final static Logger logger = LoggerFactory.getLogger(Transaction.class);

    public Transaction () {
        this.lbs = new WaitFrame[Config.getConfig().FILES_AMOUNT];
        for (int i=0; i<Config.getConfig().FILES_AMOUNT; i++) {
            this.lbs[i] = new WaitFrame(null);
        }
        this.timeStamp = new Date().getTime();
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public Transaction (DataChunk chunk) throws ClassNotFoundException, IllegalAccessException, InternalException, MalformedURLException {
        final Object[] dcs = chunk.getDcs().getValueSet();
        final Class c = this.getClass();
        final java.lang.reflect.Field[] f = c.getDeclaredFields();
        int x = 0;
        for (int i=0; i<f.length; i++) {
            final Transient ta = f[i].getAnnotation(Transient.class);
            if (ta==null) {
                final int m = f[i].getModifiers();
                if (Modifier.isPrivate(m)) {
                    f[i].setAccessible(true);
                }
                f[i].set(this, dcs[x]);
                x++;
            }
        }

        //get LBS frames
//        List<Object> bds = ixl.getObjectsByKey(this.objectId);
        ArrayList<WaitFrame> lbs = new ArrayList<WaitFrame>();

        for (int i=0; i<Config.getConfig().FILES_AMOUNT; i++) {
            lbs.add(new WaitFrame());
        }

        if (!(lbs.size()==1||lbs.size()==Config.getConfig().FILES_AMOUNT)) { //paranoid check
            throw new InternalException();
        }

        this.lbs = lbs.toArray(new WaitFrame[]{});

    }

    public void createUndoFrames(Session s, LLT llt) throws Exception {
        final Table t = Instance.getInstance().getTableByName("su.interference.persistent.UndoChunk");
        for (DataFile f : Storage.getStorage().getUndoFiles()) {
            final FrameData ub = t.createNewFrame(null, f.getFileId(), 0, 0, false, true, false, s, llt);
            setNewLB(null, ub, false);
        }
    }

    public WaitFrame getAvailableFrame(final FilePartitioned o, final boolean fpart) throws ClassNotFoundException, InstantiationException, InternalException, IllegalAccessException {
        Metrics.get("getAvailableFrame").start();
        final long st = System.currentTimeMillis();
        final long timeout = 1000;
        final int start = avframeStart.get();
        final AtomicInteger i = new AtomicInteger(start);
        while (true) {
//            for (int i=0; i<this.lbs.length; i++) {
            final WaitFrame wb = this.lbs[i.get()];
            final WaitFrame bd = fpart?wb.acquire(getTargetFileId(o.getFile())):wb.acquire();
            if (bd != null) {
                if (avframeStart.get()==this.lbs.length-1) { avframeStart.set(0); }
                else { avframeStart.getAndIncrement(); }
                Metrics.get("getAvailableFrame").stop();
                return bd;
            }
//            }
            if (System.currentTimeMillis() - st > timeout) {
                logger.error("get available frame method failed by timeout="+timeout);
                break;
            }
            if (i.get()==this.lbs.length-1) { i.set(0); } else { i.incrementAndGet(); }
        }
        Metrics.get("getAvailableFrame").stop();
        return null;
    }

    private int getTargetFileId(final int fileId) throws ClassNotFoundException, InstantiationException, InternalException, IllegalAccessException {
        for (DataFile f : Storage.getStorage().getUndoFiles()) {
            if (f.order(fileId)) {
                return f.getFileId();
            }
        }
        return 0;
    }

    public void setNewLB(FrameData frame, FrameData bd, boolean acquire) throws InternalException {
        boolean done = false;
        if (acquire) {
            for (WaitFrame wb : this.lbs) {
                if (wb.trySetBdAndAcquire(bd)) {
                    done = true;
                    break;
                }
            }
        } else {
            for (WaitFrame wb : this.lbs) {
                if (wb.trySetBd(frame, bd, 0)) {
                    done = true;
                    break;
                }
            }
        }
        if (!done) {
            throw new InternalException();
        }
    }

    public synchronized void commit (Session s, boolean remote) {
        ArrayList<Long> fptr = new ArrayList<Long>();
        final Process lsync = Instance.getInstance().getProcessByName("lsync");
        final SyncQueue syncq = (SyncQueue) lsync.getRunnable();
        if (remote) {
            try {
                this.cid = Instance.getInstance().getTableByName(this.getClass().getName()).getIncValue(s, null);
                this.transType = TRAN_THR;
                s.persist(this);
                syncq.commit();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            sendBroadcastEvents(CommandEvent.COMMIT, s);
            try {
                this.cid = Instance.getInstance().getTableByName(this.getClass().getName()).getIncValue(s, null);
                this.transType = TRAN_THR;
                s.persist(this);
                syncq.commit();

                for (TransFrame tb : tframes) {
                    boolean hasfb = false;
                    if (tb.getUframeId()>0) { // undo transframe record
                        for (Long f : fptr) {
                            if (f==tb.getUframeId()) {
                                hasfb = true;
                            }
                        }
                        final List<RetrieveLock> rls = Instance.getInstance().getRetrieveLocksByObjectId(tb.getObjectId());
                        if (rls.size()==0) {
                            final FrameData cb = Instance.getInstance().getFrameById(tb.getCframeId());
                            //deallocate undo frame
                            final FrameData ub = Instance.getInstance().getFrameById(tb.getUframeId());
                            //store frame params as free
                            if (!hasfb) {
                                final FreeFrame fb = new FreeFrame(0, tb.getUframeId(), ub.getSize());
                                s.persist(fb); //insert
                                s.delete(ub);
                                fptr.add(tb.getUframeId());
                            }
                            cb.decreaseTcounter(this.transId);
                            s.delete(tb);
                        }
                    } else { //change transframe record
                        final FrameData cb = Instance.getInstance().getFrameById(tb.getCframeId());
                        cb.setUsed(cb.getUsed()+tb.getDiff());
                        if (cb.getUsed()==0) {
                            freeFrames(cb, s);
                        } else {
                            s.persist(cb); //update new size value to dataframe
                        }
                        if (cb != null) {
                            cb.decreaseTcounter(this.transId);
                        }
                        s.delete(tb);
                    }
                }

                if (this.join != null) {
                    join.deallocate(s);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        tframes.clear();
        started = false;
        logger.info("Transaction committed");
    }

    public synchronized void rollback (Session s, boolean remote) {
        final ArrayList<FrameData> ubd1 = new ArrayList<>();
        final ArrayList<FrameData> ubd2 = new ArrayList<>();
        final ArrayList<Long> fptr = new ArrayList<>();
        if (remote) {
            try {
                this.cid = Instance.getInstance().getTableByName(this.getClass().getName()).getIncValue(s, null);
                this.transType = TRAN_THR;
                s.persist(this); //update
/*
                if (this.join != null) {
                    join.deallocate(s);
                }
*/
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            sendBroadcastEvents(CommandEvent.ROLLBACK, s);
            try {
                Collections.sort(tframes);
                for (TransFrame tb : tframes) {
                    final FrameData cb = Instance.getInstance().getFrameById(tb.getCframeId());
                    if (cb.getFrame() instanceof DataFrame) {
                        if (!ubd1.contains(cb)) {
                            ubd1.add(cb);
                        }
                    }
                    if (cb.getFrame() instanceof IndexFrame) {
                        if (!ubd2.contains(cb)) {
                            ubd2.add(cb);
                        }
                    }
                }
                for (FrameData ub : ubd1) {
                    final ArrayList<FrameData> ubs = new ArrayList<>();
                    for (TransFrame tb : tframes) {
                        if (ub.getFrameId() == tb.getCframeId()) {
                            if (tb.getUframeId() > 0) {
                                final FrameData ubb = Instance.getInstance().getFrameById(tb.getUframeId());
                                ubs.add(ubb);
                            }
                        }
                    }

                    ub.getFrame().rollbackTransaction(this, ubs, s);
                }
                for (FrameData ub : ubd2) {
                    final ArrayList<FrameData> ubs = new ArrayList<>();
                    for (TransFrame tb : tframes) {
                        if (ub.getFrameId() == tb.getCframeId()) {
                            if (tb.getUframeId() > 0) {
                                final FrameData ubb = Instance.getInstance().getFrameById(tb.getUframeId());
                                ubs.add(ubb);
                            }
                        }
                    }

                    ub.getFrame().rollbackTransaction(this, ubs, s);
                }

                for (TransFrame tb : tframes) {
                    boolean hasfb = false;
                    if (tb.getUframeId()>0) { // undo transframe record
                        for (Long f : fptr) {
                            if (f==tb.getUframeId()) {
                                hasfb = true;
                            }
                        }
                        final List<RetrieveLock> rls = Instance.getInstance().getRetrieveLocksByObjectId(tb.getObjectId());
                        if (rls.size()==0) {
                            final FrameData cb = Instance.getInstance().getFrameById(tb.getCframeId());
                            //deallocate undo frame
                            final FrameData ub = Instance.getInstance().getFrameById(tb.getUframeId());
                            //store frame params as free
                            if (!hasfb) {
                                final FreeFrame fb = new FreeFrame(0, tb.getUframeId(), ub.getSize());
                                s.persist(fb); //insert
                                s.delete(ub);
                                fptr.add(tb.getUframeId());
                            }
                            cb.decreaseTcounter(this.transId);
                            s.delete(tb);
                        }
                    } else { //change transframe record
                        final FrameData cb = Instance.getInstance().getFrameById(tb.getCframeId());
                        if (cb.getUsed()==0) {
                            logger.info("rollback freeing frame "+cb.getFile()+" "+cb.getPtr());
                            freeFrames(cb, s);
                        }
                        if (cb != null) {
                            cb.decreaseTcounter(this.transId);
                        }
                        s.delete(tb);
                    }
                }
                this.cid = Instance.getInstance().getTableByName(this.getClass().getName()).getIncValue(s, null);
                this.transType = TRAN_THR;
                s.persist(this); //update
                if (this.join != null) {
                    join.deallocate(s);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        tframes.clear();
        started = false;
        logger.info("Transaction rolled back");
    }

    @Deprecated
    public void unlockUndoFrames (int objectId, Session s) throws InternalException {
        if (this.getTransType()!=TRAN_THR) {
            throw new InternalException();  //ONLY FOR FIXED TRANSACTIONS
        }
        final ArrayList<Long> fptr = new ArrayList<Long>();
        try {
            for (TransFrame tb : tframes) {
                if (tb.getObjectId()==objectId) {
                    boolean hasfb = false;
                    if (tb.getUframeId()>0) { // undo transframe record
                        for (Long f : fptr) {
                            if (f==tb.getUframeId()) {
                                hasfb = true;
                            }
                        }
                        if (!hasfb) {
                            //deallocate undo frame
                            final FrameData ub = Instance.getInstance().getFrameById(tb.getUframeId());
                            //store frame params as free
                            final FreeFrame fb = new FreeFrame(0, tb.getUframeId(), ub.getSize());
                            s.persist(fb); //insert
                            s.delete(ub);
                            fptr.add(tb.getUframeId());
                        }
                    }
                    s.delete(tb);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void freeFrames (FrameData cb, Session s) throws Exception {
        if (cb.getUsed()>0) {
            throw new InternalException();
        }
        final Table t = Instance.getInstance().getTableById(cb.getObjectId());
        if (!t.checkLBS(cb)) { //LB can't deallocated!!! May be empty
            //check for other transactions, which locked this frame
            if (cb.getTcounterSize(this.transId) == 0) {
                throw new RuntimeException("Zero tcounter in transactional frame");
            }
            if (cb.getTcounterSize(this.transId) == 1) {
                final FreeFrame fb = new FreeFrame(0, cb.getFrameId(), cb.getSize());
                final FrameData pb = cb.getPrevFrameId()>0 ? Instance.getInstance().getFrameById(cb.getPrevFrameId()) : null;
                final FrameData nb = Instance.getInstance().getFrameById(cb.getNextFrameId());
                if (nb != null) {
                    nb.setPrevFile(pb==null?0:pb.getFile());
                    nb.setPrevFrame(pb==null?0:pb.getPtr());
                    s.persist(nb); //update
                }
                if (pb != null) {
                    pb.setNextFile(nb.getFile());
                    pb.setNextFrame(nb.getPtr());
                    s.persist(pb); //update
                }
                s.persist(fb); //insert
                s.delete(cb);
                t.decFrameAmount();
                s.persist(t);
            }
        }
    }


    public synchronized void startTransaction(Session s, LLT llt) throws Exception {
        final Transaction t = s.getTransaction();
        if (t != null && t.started) {
            throw new RuntimeException("Transaction already started for this session");
        }
        createUndoFrames(s, llt);
        if (transType==TRAN_SERIALIZABLE) {
            final Table tt = Instance.getInstance().getTableByName("su.interference.persistent.Transaction");
            this.mTran = tt.getIncValue(s, llt);
            try {
                s.persist(this, llt); //update
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        started = true;
    }
    
    protected void startStatement (final Session s) {
        if (!started) {
            try {
                startTransaction(s, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (this.transType==TRAN_READ_COMMITTED) {
            final Table t = Instance.getInstance().getTableByName("su.interference.persistent.Transaction");
            try {
                this.mTran = t.getIncValue(s, null);
                s.persist(this); //update
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void startStatement (final Session s, LLT llt) {
        final Table t = Instance.getInstance().getTableByName("su.interference.persistent.Transaction");
        if (!started) {
            try {
                startTransaction(s, llt);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (this.transType==TRAN_READ_COMMITTED) {
            try {
                this.mTran = t.getIncValue(s, llt);
                s.persist(this, llt); //update
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected void storeFrame (final FrameData cb, final FrameData ub, final int len, final Session s, LLT llt) {
        final long uframeid = ub==null?0:ub.getFrameId();
        final TransFrame tb = Instance.getInstance().getTransFrameById(this.transId, cb.getFrameId(), uframeid);

        if (tb!=null) {
            tb.setDiff(tb.getDiff()+len);
            try {
                s.persist(tb, llt); //update
            } catch (Exception e) {
                e.printStackTrace();
            }
            return;
        }

        final TransFrame ntb = new TransFrame(this.transId, cb.getObjectId(), cb.getFrameId(), uframeid);
        cb.increaseTcounter(this.transId, ntb);
        ntb.setDiff(len);
        try {
            s.persist(ntb, llt); //insert
            this.tframes.add(ntb);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendBroadcastEvents(int command, Session s) {
        try {
            TransportSyncTask.sendBroadcastCommand(command, this.transId, s);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int getNodeId() {
        return (int)this.transId%Storage.MAX_NODES;
    }

    public boolean isLocal() {
        return getNodeId() == Config.getConfig().LOCAL_NODE_ID;
    }

    protected void storeFrame (FrameData cb, int len, Session s, LLT llt) {
        storeFrame (cb, null, len, s, llt);
    }

    public long getMTran() {
        return mTran;
    }

    public long getTransId() {
        return transId;
    }

    public void setTransId(long transId) {
        this.transId = transId;
    }

    public long getSid() {
        return sid;
    }

    public void setSid(long sid) {
        this.sid = sid;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getTransType() {
        return transType;
    }

    public void setTransType(int transType) {
        this.transType = transType;
    }

    public long getCid() {
        return cid;
    }

    public SQLJoin getJoin() {
        return join;
    }

    public void setJoin(SQLJoin join) {
        this.join = join;
    }
}
