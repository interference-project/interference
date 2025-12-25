/**
 The MIT License (MIT)

 Copyright (c) 2010-2025 head systems, ltd

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
import su.interference.mgmt.MgmtClass;
import su.interference.mgmt.MgmtColumn;
import su.interference.exception.InternalException;
import su.interference.sql.SQLJoin;
import su.interference.transport.*;

import javax.persistence.*;
import java.io.Serializable;
import java.util.*;
import java.lang.reflect.Modifier;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
@DisableSync
@MgmtClass
public class Transaction implements Serializable {
    @Transient
    public static final int TRAN_READ_COMMITTED = 0;
    @Transient
    public static final int TRAN_SERIALIZABLE = 1;
    @Transient
    public static final int TRAN_THR = 9;
    @Transient
    public static final int TRAN_RBC = 10;
    @Transient
    public static final int TRAN_LEGACY = 11;
    @Transient
    public static final int TRAN_LEGACY_RBC = 12;

    @Id
    @Column
    @MapColumn
    @GeneratedValue
    @DistributedId
    @MgmtColumn(name="Transaction Id",width=10)
    private long transId;
    @Column
    @MgmtColumn(name="SID",width=10)
    @IndexColumn
    private long sid;
    @Column
    @MgmtColumn(name="Timestamp",width=10)
    private long timeStamp;
    @Column
    private int transType; // 0 - READ COMMITTED, 1 - SERIALIZABLE, 9 - THR
    @Column
    @MgmtColumn(name="MTRAN",width=10)
    private long mTran;
    @Column
    @MgmtColumn(name="Commit Id",width=10)
    private long cid;

    @MgmtColumn(name="Transaction type",width=20)
    @Transient
    private String transactionType;
    @Transient
    private final transient List<TransFrame> tframes = new CopyOnWriteArrayList<>();
    @Transient
    private final transient Set<Long> rframes = new HashSet<>();
    @Transient
    private final transient WaitFrame[] lbs;
    @Transient
    private final transient AtomicInteger avframeStart = new AtomicInteger(0);
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

    public Transaction (Transaction tran) {
        this.lbs = new WaitFrame[Config.getConfig().FILES_AMOUNT];
        for (int i=0; i<Config.getConfig().FILES_AMOUNT; i++) {
            this.lbs[i] = new WaitFrame(null);
        }
        this.transId = tran.transId;
        this.sid = tran.sid;
        this.timeStamp = tran.timeStamp;
        this.transType = tran.transType;
        this.mTran = tran.mTran;
        this.started = tran.started;
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public Transaction (DataChunk chunk) throws IllegalAccessException, InternalException {
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
        ArrayList<WaitFrame> lbs = new ArrayList<>();

        for (int i=0; i<Config.getConfig().FILES_AMOUNT; i++) {
            lbs.add(new WaitFrame());
        }

        if (!(lbs.size()==1||lbs.size()==Config.getConfig().FILES_AMOUNT)) { //paranoid check
            throw new InternalException();
        }

        this.lbs = lbs.toArray(new WaitFrame[]{});

    }

    // llt should be null, set synced flag as false for initial undo frames prevent cleanup
    public void createUndoFrames(Session s, LLT llt) throws Exception {
        final Table t = Instance.getInstance().getTableByName("su.interference.persistent.UndoChunk");
        for (DataFile f : Storage.getStorage().getUndoFiles()) {
            final FrameData ub = t.createNewFrame(null, null, f.getFileId(), 0, 0, false, true, false, s, llt);
            ub.setUnsynced();
            setNewLB(null, ub);
        }
    }

    public WaitFrame getAvailableFrame(final FilePartitioned o, final boolean fpart) throws InternalException {
        Metrics.get("getAvailableFrame").start();
        final long st = System.currentTimeMillis();
        final int a = avframeStart.get();
        int ctr = 0;

        while (true) {
            final long tp = System.currentTimeMillis() - st;
            for (int i = 0; i < this.lbs.length; i++) {
                final int i_ = (a + i) % this.lbs.length;
                final WaitFrame wb = this.lbs[i_];
                final WaitFrame bd = fpart ? wb.acquire(getTargetFileId(((FilePartitioned) o).getFile())) : wb.acquire();
                if (bd != null) {
                    avframeStart.getAndIncrement();
                    Metrics.get("getAvailableFrame").stop();
                    return bd;
                }
                ctr++;
            }
            if (ctr > Config.getConfig().CHECK_AVAIL_FRAME_ATTEMPTS) {
                //file-depends acquire fails - try forced acquire
                for (int i = 0; i < this.lbs.length; i++) {
                    final int i_ = (a + i) % this.lbs.length;
                    final WaitFrame wb = this.lbs[i_];
                    final WaitFrame bd = wb.acquire();
                    if (bd != null) {
                        avframeStart.getAndIncrement();
                        Metrics.get("getAvailableFrame").stop();
                        return bd;
                    }
                }

                //critical stop
                for (int i = 0; i < this.lbs.length; i++) {
                    logger.warn("lbs: "+lbs[i].getBd().getFrameId()+":"+lbs[i].getBusy().get());
                }
                logger.warn("avframestart: "+avframeStart.get());
                logger.warn("number of attempts exceeded for getavailableframe method: " + Config.getConfig().CHECK_AVAIL_FRAME_ATTEMPTS);
                break;
            }
/*
            if (tp > Config.getConfig().CHECK_AVAIL_FRAME_TIMEOUT) {
                //file-depends acquire fails - try forced acquire
                for (int i = 0; i < this.lbs.length; i++) {
                    final int i_ = (a + i) % this.lbs.length;
                    final WaitFrame wb = this.lbs[i_];
                    final WaitFrame bd = wb.acquire();
                    if (bd != null) {
                        avframeStart.getAndIncrement();
                        Metrics.get("getAvailableFrame").stop();
                        return bd;
                    }
                }

                //critical stop
                for (int i = 0; i < this.lbs.length; i++) {
                    logger.warn("lbs: "+lbs[i].getBd().getFrameId()+":"+lbs[i].getBusy().get());
                }
                logger.warn("avframestart: "+avframeStart.get());
                logger.warn("timeout occured during getavailableframe method: " + Config.getConfig().CHECK_AVAIL_FRAME_TIMEOUT);
                break;
            }
*/
        }
        Metrics.get("getAvailableFrame").stop();
        return null;
    }

    private int getTargetFileId(final int fileId) throws InternalException {
        for (DataFile f : Storage.getStorage().getUndoFiles()) {
            if (f.order(fileId)) {
                return f.getFileId();
            }
        }
        return 0;
    }

    public void setNewLB(FrameData frame, FrameData bd) throws InternalException {
        boolean done = false;
        for (WaitFrame wb : this.lbs) {
            if (wb.trySetBd(frame, bd, 0)) {
                done = true;
                break;
            }
        }
        if (!done) {
            throw new InternalException();
        }
    }

    public synchronized void commit (Session s, boolean remote) {
        final Process lsync = Instance.getInstance().getProcessByName("lsync");
        final SyncQueue syncq = (SyncQueue) lsync.getRunnable();
        if (remote) {
            if (isLocal()) {
                logger.warn("remote commit should not be applied to transaction on init node");
            }
        } else {
            if (isLocal()) {
                sendBroadcastEvents(CommandEvent.COMMIT, s);
            } else {
                logger.warn("commit should not be applied to transaction on remote node");
            }
        }

        try {
            for (Long frameId : rframes) {
                Instance.getInstance().getFrameById(frameId).decreaseTcounter(this.transId);
            }
        } catch (Exception e) {
            logger.error("exception occured during transaction commit", e);
        }

        try {
            for (TransFrame tb : tframes) {
                if (tb.getUframeId() > 0) { // undo transframe record
                    final List<RetrieveLock> rls = Instance.getInstance().getRetrieveLocksByObjectId(tb.getObjectId());
                    if (rls.size() == 0) {
                        final FrameData cb = Instance.getInstance().getFrameById(tb.getCframeId());
                        cb.decreaseTcounter(this.transId);
                    }
                } else { //change transframe record
                    final FrameData cb = Instance.getInstance().getFrameById(tb.getCframeId());
                    cb.setUsed(cb.getUsed() + tb.getDiff());
                    cb.decreaseTcounter(this.transId);
                    if (cb.getUsed() == 0) {
                        freeFrames(cb, s);
                    } else {
                        s.persist(cb); //update new size value to dataframe
                    }
                }
            }

        } catch (Exception e) {
            logger.error("exception occured during transaction commit", e);
        }

        try {
            this.cid = Instance.getInstance().getTableByName(this.getClass().getName()).getIncValue(s, null);
            this.transType = TRAN_THR;
            s.persist(this);
            syncq.commit();

            if (!remote) {
                if (isLocal()) {
                    if (this.join != null) {
                        join.deallocate(s);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("exception occured during transaction commit", e);
        }

        rframes.clear();
        tframes.clear();
        started = false;
        logger.info("Transaction committed");
    }

    @SuppressWarnings("unchecked")
    public synchronized void rollback (Session s, boolean remote) {
        //final ArrayList<FrameData> ubd1 = new ArrayList<>();
        //final ArrayList<FrameData> ubd2 = new ArrayList<>();
        final Map<Long, List<FrameData>> ubd1 = new HashMap<>();
        final Map<Long, List<FrameData>> ubd2 = new HashMap<>();
        final Map<Integer, List<Long>> fmap = new HashMap<>();

        if (remote) {
            if (isLocal()) {
                logger.warn("remote rollback should not be applied to transaction on init node");
            }
        } else {
            if (!isLocal()) {
                logger.warn("rollback should not be applied to transaction on remote node");
            }
        }

        try {
            for (Long frameId : rframes) {
                Instance.getInstance().getFrameById(frameId).decreaseTcounter(this.transId);
            }
/*
                if (this.join != null) {
                    join.deallocate(s);
                }
*/
        } catch (Exception e) {
            logger.error("exception occured during transaction rollback", e);
        }

        try {
            Collections.sort(tframes);

            logger.info("transaction frames sorted");

            for (TransFrame tb : tframes) {
                final FrameData cb = Instance.getInstance().getFrameById(tb.getCframeId());
                if (cb.isIndex()) {
                    if (ubd2.get(tb.getCframeId()) == null) {
                        List<FrameData> fdl = new ArrayList<>();
                        if (tb.getUframeId() > 0) {
                            fdl.add(Instance.getInstance().getFrameById(tb.getUframeId()));
                        }
                        ubd2.put(tb.getCframeId(), fdl);
                    } else {
                        if (tb.getUframeId() > 0) {
                            ubd2.get(tb.getCframeId()).add(Instance.getInstance().getFrameById(tb.getUframeId()));
                        }
                    }
                } else {
                    if (ubd1.get(tb.getCframeId()) == null) {
                        List<FrameData> fdl = new ArrayList<>();
                        if (tb.getUframeId() > 0) {
                            fdl.add(Instance.getInstance().getFrameById(tb.getUframeId()));
                        }
                        ubd1.put(tb.getCframeId(), fdl);
                    } else {
                        if (tb.getUframeId() > 0) {
                            ubd1.get(tb.getCframeId()).add(Instance.getInstance().getFrameById(tb.getUframeId()));
                        }
                    }
                }
            }

            logger.info("transaction frames maps prepared");

/*
            for (TransFrame tb : tframes) {
                final FrameData cb = Instance.getInstance().getFrameById(tb.getCframeId());
                if (cb.isIndex()) {
                    if (!ubd2.contains(cb)) {
                        ubd2.add(cb);
                    }
                } else {
                    if (!ubd1.contains(cb)) {
                        ubd1.add(cb);
                    }
                }
            }
*/

            for (Map.Entry<Long, List<FrameData>> entry : ubd2.entrySet()) {
                final FrameData ub = Instance.getInstance().getFrameById(entry.getKey());
/*
                final ArrayList<FrameData> ubs = new ArrayList<>();
                for (TransFrame tb : tframes) {
                    if (ub.getFrameId() == tb.getCframeId()) {
                        if (tb.getUframeId() > 0) {
                            final FrameData ubb = Instance.getInstance().getFrameById(tb.getUframeId());
                            ubs.add(ubb);
                        }
                    }
                }
*/

                if (ub.isIndex()) {
//                    ub.setRbck(true);
                    ub.rollbackTransaction(this, entry.getValue(), s);
                }
            }

            for (Map.Entry<Long, List<FrameData>> entry : ubd1.entrySet()) {
                final FrameData ub = Instance.getInstance().getFrameById(entry.getKey());
/*
                final ArrayList<FrameData> ubs = new ArrayList<>();
                for (TransFrame tb : tframes) {
                    if (ub.getFrameId() == tb.getCframeId()) {
                        if (tb.getUframeId() > 0) {
                            final FrameData ubb = Instance.getInstance().getFrameById(tb.getUframeId());
                            ubs.add(ubb);
                        }
                    }
                }
*/

                if (!ub.isIndex()) {
                    ub.rollbackTransaction(this, entry.getValue(), s);
                }
            }

            for (Map.Entry<Long, List<FrameData>> entry : ubd2.entrySet()) {
                final FrameData ub = Instance.getInstance().getFrameById(entry.getKey());
                final Frame frame_ = ub.getFrame();
                if (frame_ instanceof IndexFrame) {
                    ub.setRbck(false);
                    ((IndexFrame) frame_).cleanICEntities();
                }
            }

            for (TransFrame tb : tframes) {
                if (tb.getUframeId() > 0) { // undo transframe record
                    final List<RetrieveLock> rls = Instance.getInstance().getRetrieveLocksByObjectId(tb.getObjectId());
                    if (rls.size() == 0) {
                        final FrameData cb = Instance.getInstance().getFrameById(tb.getCframeId());
                        if (cb != null) {
                            cb.decreaseTcounter(this.transId);
                        }
                    }
                } else { //change transframe record
                    final FrameData cb = Instance.getInstance().getFrameById(tb.getCframeId());
                    cb.decreaseTcounter(this.transId);
                    if (cb.getUsed() == 0) {
                        if (fmap.get(cb.getObjectId()) == null) {
                            fmap.put(cb.getObjectId(), new ArrayList<>());
                        }
//                        fmap.get(cb.getObjectId()).add(cb.getFrameId());
                        logger.info("rollback freeing frame " + cb.getFile() + " " + cb.getPtr());
                        freeFrames(cb, s);
                    }
                }
            }
            if (this.join != null) {
                join.deallocate(s);
            }
        } catch (Exception e) {
            logger.error("exception occured during transaction rollback", e);
        }

        try {
            //post-rollback check
            Map<Long, Integer> checkmap = new HashMap<>();
            int is_ok = 0;
            int is_fail = 0;
            for (TransFrame tf : Instance.getInstance().getTransFramesByTransId(this.transId)) {
                if (checkmap.get(tf.getCframeId()) == null) {
                    FrameData bd = Instance.getInstance().getFrameById(tf.getCframeId());
                    if (bd != null) { //frame already freed
                        checkmap.put(tf.getCframeId(), bd.checkTcounter(this.transId));
                    }
                }
            }
            for (Map.Entry<Long, Integer> entry : checkmap.entrySet()) {
                if (entry.getValue() >= 0) {
                    is_fail++;
                    logger.warn("check failed "+entry.getValue()+" on frame "+entry.getKey());
                } else {
                    is_ok++;
                }
            }
            logger.info("check frames failed: "+is_fail);
            logger.info("check frames ok: "+is_ok);

            this.cid = Instance.getInstance().getTableByName(this.getClass().getName()).getIncValue(s, null);
            this.transType = TRAN_RBC;
            s.persist(this);

/*
            for (Map.Entry<Integer, List<Long>> entry : fmap.entrySet()) {
                Table t = Instance.getInstance().getTableById(entry.getKey());
                t.freeFrames(entry.getValue(), s, this, "rollback");
            }
*/
        } catch (Exception e) {
            logger.error("exception occured during transaction rollback", e);
        }

        if (!remote) {
            if (isLocal()) {
                sendBroadcastEvents(CommandEvent.ROLLBACK, s);
            }
        }

        rframes.clear();
        tframes.clear();
        started = false;
        logger.info("Transaction rolled back");
    }

    @Deprecated
    public void unlockUndoFrames (int objectId, Session s) throws InternalException {
        if (this.getTransType() != TRAN_THR && this.getTransType() != TRAN_RBC) {
            throw new InternalException();  //ONLY FOR FIXED TRANSACTIONS
        }
        final ArrayList<Long> fptr = new ArrayList<>();
        try {
            for (TransFrame tb : tframes) {
                if (tb.getObjectId()==objectId) {
                    boolean hasfb = false;
                    if (tb.getUframeId() >0 ) { // undo transframe record
                        for (Long f : fptr) {
                            if (f == tb.getUframeId()) {
                                hasfb = true;
                                break;
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
            logger.error("exception occured during unlock undo frames", e);
        }
    }

    public void freeFrames (FrameData cb, Session s) throws Exception {
        if (cb.getUsed()>0) {
            throw new InternalException();
        }
        final Table t = Instance.getInstance().getTableById(cb.getObjectId());
        if (!t.checkLBS(cb)) { //LB can't deallocated!!! May be empty
            //check for other transactions, which locked this frame
            if (!cb.isFrameBusy()) {
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

    public void storeRFrame(long uframeId, long frameId, int objectId, Session s) throws Exception {
        rframes.add(frameId);
        final TransFrame tb = Instance.getInstance().getTransFrameById(this.transId, frameId, uframeId);
        if (tb == null) {
            final TransFrame ntb = new TransFrame(this.transId, objectId, frameId, uframeId);
            s.persist(ntb);
        }
    }

    public void retrieveTframes() {
        tframes.clear();
        for (TransFrame tf : Instance.getInstance().getTransFramesByTransId(this.transId)) {
            FrameData bd = Instance.getInstance().getFrameById(tf.getCframeId());
            bd.increaseTcounter(this.transId, tf);
            if (this.isLocal()) {
                tframes.add(tf);
            } else {
                if (tf.getUframeId() > 0) {
                    rframes.add(tf.getCframeId());
                }
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
                logger.error("exception occured during start transaction", e);
            }
        }
        started = true;
    }
    
    protected void startStatement (final Session s) {
        if (!started) {
            try {
                startTransaction(s, null);
            } catch (Exception e) {
                logger.error("exception occured during start statement", e);
            }
        }
        if (this.transType==TRAN_READ_COMMITTED) {
            final Table t = Instance.getInstance().getTableByName("su.interference.persistent.Transaction");
            try {
                this.mTran = t.getIncValue(s, null);
                s.persist(this); //update
            } catch (Exception e) {
                logger.error("exception occured during start statement", e);
            }
        }
    }

    protected void startStatement (final Session s, LLT llt) {
        final Table t = Instance.getInstance().getTableByName("su.interference.persistent.Transaction");
        if (!started) {
            try {
                startTransaction(s, llt);
            } catch (Exception e) {
                logger.error("exception occured during start statement", e);
            }
        }
        if (this.transType==TRAN_READ_COMMITTED) {
            try {
                this.mTran = t.getIncValue(s, llt);
                s.persist(this, llt); //update
            } catch (Exception e) {
                logger.error("exception occured during start statement", e);
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
                logger.error("exception occured during store frame", e);
            }
            return;
        }

        final TransFrame ntb = new TransFrame(this.transId, cb.getObjectId(), cb.getFrameId(), uframeid);
        cb.increaseTcounter(this.transId, ntb);
        ntb.setDiff(len);
        try {
            s.persist(ntb, llt); //insert
            synchronized (this) {
                this.tframes.add(ntb);
            }
        } catch (Exception e) {
            logger.error("exception occured during store frame", e);
        }
    }

    private void sendBroadcastEvents(int command, Session s) {
        try {
            TransportSyncTask.sendBroadcastCommand(command, this.transId, s);
        } catch (Exception e) {
            logger.error("exception occured during send broadcast events", e);
        }
    }

    public int getNodeId() {
        final long n = this.transId%Storage.MAX_NODES;
        return (int)n;
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

    public String getTransactionType() {
        switch (this.transType) {
            case TRAN_READ_COMMITTED:
                return "READ COMMITTED";
            case TRAN_SERIALIZABLE:
                return "SERIALIZABLE";
            case TRAN_THR:
                return "COMMITTED";
            case TRAN_RBC:
                return "ROLLED BACK";
            case TRAN_LEGACY:
                return "COMMITTED/CLEAN";
            case TRAN_LEGACY_RBC:
                return "ROLLED BACK/CLEAN";
        }
        return "N/A";
    }
}
