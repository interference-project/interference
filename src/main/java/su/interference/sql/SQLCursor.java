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

package su.interference.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.Config;
import su.interference.core.IndexDescript;
import su.interference.persistent.*;
import su.interference.exception.InternalException;
import su.interference.core.Instance;
import su.interference.transport.TransportContext;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLCursor implements FrameIterator {

    private final int id;
    private ResultSet target;
    private static ExecutorService exec = SQLJoinThreadPool.getThreadPool();
    private static ExecutorService exec2 = SQLJoinThreadPool.getThreadPool2();
    private static ExecutorService remotepool = Executors.newCachedThreadPool();
    private static ExecutorService streampool = Executors.newCachedThreadPool();
    private static ExecutorService groupspool = Executors.newCachedThreadPool();
    private static ExecutorService rspool = Executors.newCachedThreadPool();
    private final LinkedBlockingQueue<FrameApiJoin> ltasks;
    private final LinkedBlockingQueue<FrameApiJoin> rtasks;
    private final LinkedBlockingQueue<FrameApiJoin> rtasks_;
    boolean ldone = false;
    boolean rdone = false;
    private FrameData bdnext;
    private FrameHolder current;
    private boolean sent;
    private Cursor cur;
    private Session s;
    private List<SQLColumn> rscols;
    private NestedCondition nc;
    private boolean last;
    private boolean peristent;
    private boolean flush;
    private final SQLJoinDispatcher hmap;
    private final FrameIterator lbi;
    private final FrameIterator rbi;
    private final FrameIterator pbi;
    private final List<Integer> objectIds;
    private String rightType;
    private boolean leftFS;
    private final boolean furtherUseUC;
    private final SQLColumn joinedCC;
    private SQLColumn extJoinedCC;
    private final IndexDescript leadingIndex;
    private final boolean process;
    private final Class evtprc;

    private static Map<Integer, Map<Long, ConcurrentLinkedQueue<FrameApi>>> sfmap = new ConcurrentHashMap<>();
    private static final int BATCH_SIZE = 8;
    private static final int REMOTE_TASK_SIZE = 1000;
    private final static Logger logger = LoggerFactory.getLogger(SQLCursor.class);

    public SQLCursor (int id, FrameIterator lbi, FrameIterator rbi, NestedCondition nc, List<SQLColumn> rscols, boolean ixflag, boolean last, Cursor cur, IndexDescript leadingIndex, Session s) throws Exception {
        this.id = id;
        this.objectIds = new ArrayList<>();
        this.objectIds.addAll(lbi.getObjectIds());
        if (rbi != null) {
            this.objectIds.addAll(rbi.getObjectIds());
        }
        this.s = s;
        this.cur = cur;
        this.nc = nc;
        this.last = last;
        this.leadingIndex = leadingIndex;
        this.process = lbi.isProcess();
        this.evtprc = lbi.getEventProcessor();

        //ordered sets should be persistent
        this.peristent = cur.getSqlStmt().isEntityResult() ? false : ixflag || !last;

        if (cur.isStream()) {
            final Map<Long, ConcurrentLinkedQueue<FrameApi>> sfmap_ = sfmap.get(lbi.getObjectId());
            if (sfmap_ == null) {
                final Map<Long, ConcurrentLinkedQueue<FrameApi>> sfmap__ = new ConcurrentHashMap<>();
                sfmap__.put(s.getId(), new ConcurrentLinkedQueue<>());
                sfmap.put(lbi.getObjectId(), sfmap__);
            } else {
                sfmap_.put(s.getId(), new ConcurrentLinkedQueue<>());
            }
        }

        //todo wrong case - column set must be rebuilded for prevent bad indexes intersect cases
        //todo need to refactor SQLJoin - extJoinedCC must be set on construct time

//        if (cur.getType() == Cursor.SLAVE_TYPE && cur.getResultTargetNames() != null && this.id == 1) {
        if (cur.getType() == Cursor.SLAVE_TYPE && cur.getResultTargetNames() != null) {
            target = new ResultSetImpl(cur.getSqlStmt().isEntityResult() ? cur.getSqlStmt().getEntityTable() : s.registerTable(cur.getResultTargetNames().get(id-1), null, s, rscols, null, null, ixflag && last), this, this.peristent);
        } else if (cur.getType() == Cursor.STREAM_TYPE) {
            final List<SQLColumn> rscols_ = getIOTCList();
            final Table rstable = cur.getSqlStmt().isEntityResult() ? cur.getSqlStmt().getEntityTable() : s.registerTable("su.interference.persistent.R$" + UUID.randomUUID().toString().replace('-', '$'), null, s, rscols_, null, null, ixflag && last);
            target = new StreamQueue(rscols_, rstable, cur.getSqlStmt().getCols().getWindowColumn(), s);
        } else {
            target = new ResultSetImpl(cur.getSqlStmt().isEntityResult() ? cur.getSqlStmt().getEntityTable() : s.registerTable("su.interference.persistent.R$" + UUID.randomUUID().toString().replace('-', '$'), null, s, rscols, null, null, ixflag && last), this, this.peristent);
        }
        current = new FrameHolder(target);

        ltasks = new LinkedBlockingQueue<>();
        rtasks = new LinkedBlockingQueue<>();
        rtasks_ = new LinkedBlockingQueue<>();

        //rebuild column set for sqlcursor iterator
        final SQLCursor cursor_ = lbi.getType() == FrameIterator.TYPE_CURSOR ? (SQLCursor) lbi : rbi != null && rbi.getType() == FrameIterator.TYPE_CURSOR ? (SQLCursor) rbi : null;
        if (cursor_ != null) {
            ArrayList<SQLColumn> rscols_ = new ArrayList<>();
            for (SQLColumn sqlc : rscols) {
                if (cursor_.getObjectIds().contains(sqlc.getObjectId())) {
                    final Table t_ = ((ResultSetImpl) cursor_.getTarget()).getTarget();
                    final SQLColumn sqlc_ = new SQLColumn(t_, sqlc.getId(), getTargetColumn(sqlc), sqlc.getAlias(), sqlc.getFtype(), sqlc.getLoc(), sqlc.getOrderOrd(), sqlc.getGroupOrd(), sqlc.getWindowInterval(), true, cursor_.isFurtherUseUC());
                    rscols_.add(sqlc_);
                } else {
                    rscols_.add(sqlc);
                }
            }
            this.rscols = rscols_;
        } else {
            this.rscols = rscols;
        }

        hmap = nc.getJoinDispatcher(lbi, rbi, this.rscols, this.leadingIndex, this.process, s);

        //todo move to SQLJoinDispatcher
        if (hmap == null && rbi == null) {
            final ValueCondition vc = nc.getIndexVC(lbi, null);
            if (vc != null) {
                final Table lt = Instance.getInstance().getTableById(lbi.getObjectId());
                this.lbi = new SQLIndex(vc.getConditionColumn().getIndex(), lt, true, vc.getConditionColumn(), vc.getConditionColumn(), false, nc, 0, this.process, s);
                this.rbi = rbi;
            } else if (this.leadingIndex != null && lbi.getObjectId() == leadingIndex.getT().getObjectId()) {
                final Table lt = Instance.getInstance().getTableById(lbi.getObjectId());
                this.lbi = new SQLIndex(leadingIndex.getIndex(), lt, true, null, null, false, nc, 0, true, this.process, s);
                this.leadingIndex.accept();
                this.rbi = null;
            } else {
                this.lbi = lbi;
                this.rbi = null;
            }
        } else {
            this.lbi = hmap == null ? lbi : hmap.getLbi();
            this.rbi = hmap == null ? rbi : hmap.getRbi();
        }

        if (this.leadingIndex != null && this.leadingIndex.isAccepted()) {
            this.target.clearPersistent();
        }

        //may use UC in cursor for hash map iterators
        this.furtherUseUC = hmap==null?false:hmap.isFurtherUseUC();
        this.joinedCC = hmap==null?null:hmap.getJoinedCC();
        final Table lt = Instance.getInstance().getTableById(this.lbi.getObjectId());
        if (this.lbi instanceof SQLTable) {
            logger.info("use full scan for "+lt.getName());
            this.leftFS = true;
            this.lbi.setLeftfs(true);
        }
        if (this.lbi instanceof SQLCursor) {
            logger.info("use full scan for " + lt.getName() +" persistent = "+((SQLCursor) lbi).getTarget().isPersistent());
        }

        if (this.rbi != null) {
            final Table rt = Instance.getInstance().getTableById(this.rbi.getObjectId());
            if (this.rbi instanceof SQLTable) {
                logger.info("use full scan for "+rt.getName());
            }
            if (this.rbi instanceof SQLCursor) {
                logger.info("use full scan for " + rt.getName() +" persistent = "+((SQLCursor) lbi).getTarget().isPersistent());
            }
        }
        this.pbi = this.rbi != null && this.rbi.isProcess() ? this.rbi : this.lbi;
    }

    public int getType() {
        return FrameIterator.TYPE_CURSOR;
    }
    public boolean isIndex() throws MalformedURLException, ClassNotFoundException { return target.isIndex(); }

    public int getObjectId() {
        return target.getObjectId();
    }

    protected FrameJoinTask buildFrameJoinTask(FrameIterator pbi, FrameApi bd1, FrameApi bd2, FrameApiJoin j) throws Exception {
        return new FrameJoinTask(cur, pbi, bd1, bd2, target, rscols, nc, id, last, hmap, j, s);
    }

    public void build() {
        final Integer[] ns = Config.getConfig().TEST_DISTRIBUTE_MODE == 0 ? TransportContext.getInstance().getOnlineNodesWithLocal() : TransportContext.getInstance().getNodesWithLocal();
        final boolean isc = this.lbi instanceof SQLCursor || this.rbi instanceof SQLCursor;
        final SQLCursor c1 = this;
        final LinkedBlockingQueue<FrameApiJoin> rtasks__ = new LinkedBlockingQueue<>();

        Runnable build = new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName("interference sql cursor build "+Thread.currentThread().getId());
                int i = 0;
                try {
                    while (lbi.hasNextFrame()) {
                        final FrameApi bd1 = lbi.nextFrame();

                        if (bd1 != null) {
                            if (rbi == null) {
                                if (lbi.noDistribute()) {
                                    ltasks.put(new FrameApiJoin(Config.getConfig().LOCAL_NODE_ID, c1, lbi, bd1, null));
                                } else {
                                    if (ns[i] == Config.getConfig().LOCAL_NODE_ID) {
                                        ltasks.put(new FrameApiJoin(ns[i], c1, lbi, bd1, null));
                                    } else {
                                        rtasks__.put(new FrameApiJoin(ns[i], c1, lbi, bd1, null));
                                    }
                                }
                                i++;
                                if (i == ns.length) {
                                    i = 0;
                                }
                            } else {
                                while (rbi.hasNextFrame()) {
                                    final FrameApi bd2 = rbi.nextFrame();
                                    if (bd2 != null) {
                                        if (rightType == null) {
                                            rightType = bd2.getClass().getSimpleName();
                                        }
                                        if (lbi.noDistribute() || rbi.noDistribute()) {
                                            ltasks.put(new FrameApiJoin(Config.getConfig().LOCAL_NODE_ID, c1, pbi, bd1, bd2));
                                        } else {
                                            if (ns[i] == Config.getConfig().LOCAL_NODE_ID) {
                                                ltasks.put(new FrameApiJoin(ns[i], c1, pbi, bd1, bd2));
                                            } else {
                                                rtasks__.put(new FrameApiJoin(ns[i], c1, pbi, bd1, bd2));
                                            }
                                        }
                                        i++;
                                        if (i == ns.length) {
                                            i = 0;
                                        }
                                    }
                                }
                                rbi.resetIterator();
                            }
                        }
                    }
                    ltasks.put(new FrameApiJoin());
                    rtasks__.put(new FrameApiJoin());
                } catch (Exception e) {
                    logger.error("exception occured during cursor build", e);
                }
            }
        };
        remotepool.submit(build);

        if (!sent) {
            Runnable send = new Runnable() {
                @Override
                public void run() {
                    Thread.currentThread().setName("interference-sql-cursor-remote-send-"+Thread.currentThread().getId());
                    try {
                        final Map<Integer, Map<String, FrameApiJoin>> joins = new HashMap<>();
                        for (Integer nodeId : ns) {
                            if (nodeId != Config.getConfig().LOCAL_NODE_ID) {
                                joins.put(nodeId, new HashMap<>());
                            }
                        }

                        boolean cnue = true;
                        while (cnue) {
                            final FrameApiJoin j = rtasks__.take();
                            if (j.isTerminate()) {
                                cnue = false;
                            } else {
                                if (j.getNodeId() != Config.getConfig().LOCAL_NODE_ID) {
                                    joins.get(j.getNodeId()).put(j.getKey(), j);
                                    if (joins.get(j.getNodeId()).size() == REMOTE_TASK_SIZE) {
                                        final RemoteTask rt = new RemoteTask(cur, j.getNodeId(), joins.get(j.getNodeId()), rightType, target.getTableClass() == null ? null : target.getTableClass().getName());
                                        remotepool.submit(rt);
                                        for (Map.Entry<String, FrameApiJoin> entry : joins.get(j.getNodeId()).entrySet()) {
                                            rtasks.put(entry.getValue());
                                        }
                                        joins.put(j.getNodeId(), new HashMap<>());
                                    }
                                }
                            }
                        }

                        for (Integer nodeId : ns) {
                            if (nodeId != Config.getConfig().LOCAL_NODE_ID) {
                                if (joins.get(nodeId).size() > 0) {
                                    final RemoteTask rt = new RemoteTask(cur, nodeId, joins.get(nodeId), rightType, target.getTableClass() == null ? null : target.getTableClass().getName());
                                    remotepool.submit(rt);
                                    for (Map.Entry<String, FrameApiJoin> entry : joins.get(nodeId).entrySet()) {
                                        rtasks.put(entry.getValue());
                                    }
                                }
                            }
                        }
                        rtasks.put(new FrameApiJoin());
                    } catch (Exception e) {
                        logger.error("exception occured during remote send of tasks", e);
                    }
                    sent = true;
                }
            };
            remotepool.submit(send);
        }
    }

    public void stream() throws Exception {
        final Queue<FrameApi> q = sfmap.get(lbi.getObjectId()).get(s.getId());

        if (!cur.isStream()) {
            logger.error("wrong stream method call: SQL statement is not a stream");
        }
        if (q == null) {
            throw new RuntimeException("internal error: queue not exist for object id = "+lbi.getObjectId());
        }
        if (!(target instanceof StreamQueue)) {
            throw new RuntimeException("internal error: wrong target type for object id = "+lbi.getObjectId());
        }
        s.setStream(true);
        final Table gtable = ((StreamQueue) target).getRstable();
        Runnable r = new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName("interference-sql-stream-thread-"+Thread.currentThread().getId());
                try {
                    FrameGroupTask group = null;
                    if (cur.getSqlStmt().isGroupedResult()) {
                        if (group == null) {
                            group = new FrameGroupTask(cur, target, gtable, s);
                            groupspool.submit(group);
                        }
                    }

                    //todo FrameContainer should use indexes
                    List<FrameApi> flist = new ArrayList<>();
                    while (lbi.hasNextFrame()) {
                        final FrameApi bd1 = lbi.nextFrame();
                        if (bd1 != null) {
                            flist.add(bd1);
                        }
                    }

                    if (flist.size() > 0) {
                        final ContainerFrame cf = new ContainerFrame(lbi.getObjectId(), flist);
                        final FrameIterator pbi = rbi != null && rbi.isProcess() ? rbi : lbi;
                        final FrameJoinTask task = new FrameJoinTask(cur, pbi, cf, null, target, ((StreamQueue) target).getRscols(), nc, id, last, null, null, s);
                        exec.submit(task);

                        boolean cnue =  true;
                        while (cnue) {
                            final Object o = task.getQ().take();
                            if (o instanceof ResultSetTerm) {
                                cnue = false;
                            } else {
                                if (cur.getSqlStmt().isGroupedResult()) {
                                    group.put(o);
                                } else {
                                    target.persist(o, s);
                                }
                            }
                        }
                    }

                    while (((StreamQueue) target).isRunning()) {
                        FrameApi f = q.poll();
                        if (f != null) {
                            final FrameIterator pbi = rbi != null && rbi.isProcess() ? rbi : lbi;
                            final FrameJoinTask task = new FrameJoinTask(cur, pbi, f, null, target, ((StreamQueue) target).getRscols(), nc, id, last, null, null, s);
                            exec.submit(task);

                            boolean cnue =  true;
                            while (cnue) {
                                final Object o = task.getQ().take();
                                if (o instanceof ResultSetTerm) {
                                    cnue = false;
                                } else {
                                    if (cur.getSqlStmt().isGroupedResult()) {
                                        group.put(o);
                                    } else {
                                        target.persist(o, s);
                                    }
                                }
                            }
                        }
                        if (q.peek() == null) {
                            Thread.sleep(100);
                        }
                    }

                    target.persist(new ResultSetTerm(), s);
                } catch (Exception e) {
                    ((StreamQueue) target).stop();
                    logger.error("exception occured during sql stream", e);
                }
            }
        };
        streampool.submit(r);
    }

    public static void addStreamFrame(FrameApi f) {
        final Map<Long, ConcurrentLinkedQueue<FrameApi>> sfmap_ = sfmap.get(f.getObjectId());
        if (sfmap_ != null) {
            for (Map.Entry<Long, ConcurrentLinkedQueue<FrameApi>> entry : sfmap_.entrySet()) {
                final Queue<FrameApi> q = entry.getValue();
                if (q != null) {
                    q.add(f);
                }
            }
        }
    }

    public static void removeStreamQueue(Session s) {
        for (Map.Entry<Integer, Map<Long, ConcurrentLinkedQueue<FrameApi>>> entry :  sfmap.entrySet()) {
            final Map<Long, ConcurrentLinkedQueue<FrameApi>> sfmap_ = entry.getValue();
            sfmap_.remove(s.getId());
        }
    }

    // execute single task (on local node only)
/*
    public List<Object> execute(FrameApi bd1, FrameApi bd2) throws Exception {
        final FrameJoinTask task = new FrameJoinTask(cur, bd1, bd2, target, rscols, nc, id, Config.getConfig().LOCAL_NODE_ID, last, lbi.isLeftfs(), hmap, s);
        final Future<List<Object>> f = exec.submit(task);
        return f.get();
    }
*/

    public Future<BlockingQueue<Object>> execute(FrameIterator pbi, FrameApi bd1, FrameApi bd2, FrameApiJoin j) throws Exception {
        final FrameJoinTask task = new FrameJoinTask(cur, pbi, bd1, bd2, target, rscols, nc, id, last, hmap, j, s);
        return exec.submit(task);
    }

    public synchronized FrameData nextFrame() {
        return bdnext;
    }

    private synchronized FrameData nextFrame2() throws InternalException {
        boolean done = ldone && rdone;
        boolean prcrj = true;
        FrameData ret = current.getFrame(ldone && rdone);

        if (ret != null) {
            return ret;
        }

        try {

            while (!done) {
                final ArrayList<FrameApiJoin> flist = new ArrayList<>();
                final ArrayList<Future<FrameApiJoin>> flist2 = new ArrayList<>();

                for (int i = 0; i < BATCH_SIZE; i++) {
                    if (!ldone) {
                        final FrameApiJoin j = ltasks.take();
                        if (j.isTerminate()) {
                            ldone = true;
                        } else {
                            exec2.submit(j);
                            flist.add(j);
                        }
                    }
                }

                for (int i = 0; i < BATCH_SIZE; i++) {
                    if (!rdone) {
                        final FrameApiJoin j = rtasks.take();
                        if (j.isTerminate()) {
                            rdone = true;
                        } else {
                            flist2.add(exec2.submit(j));
                        }
                    }
                }

//                for (int i=0; i < BATCH_SIZE; i++) {
                if (!prcrj) {
                    final FrameApiJoin j = rtasks_.take();
                    if (j.isTerminate()) {
                        prcrj = true;
                    } else {
                        exec2.submit(j);
                        flist.add(j);
                    }
                }
//                }

                for (FrameApiJoin j : flist) {
                    try {
                        //final FrameApiJoin j = f.get();
                        final BlockingQueue<Object> q = j.getResult();
                        boolean cnue = true;
                        while (cnue) {
                            final Object o = q.take();
                            if (o instanceof ResultSetTerm) {
                                cnue = false;
                            } else {
                                target.persist(o, s);
                            }
                        }
                    } catch (Exception e) {
                        if (e instanceof ExecutionException) {
                            logger.error("exception occured during cursor execution", e.getCause());
                        } else {
                            logger.error("exception occured during cursor execution", e);
                        }
                    }
                }

                for (Future<FrameApiJoin> f : flist2) {
                    try {
                        final FrameApiJoin j = f.get();
                        if (j.isFailed()) {
                            FrameApiJoin j_ = new FrameApiJoin(Config.getConfig().LOCAL_NODE_ID, this, pbi, j.getBd1(), j.getBd2());
                            rtasks_.put(j_);
                            prcrj = false;
                        } else {
                            final BlockingQueue<Object> q = j.getResult();
                            boolean cnue = true;
                            while (cnue) {
                                final Object o = q.take();
                                if (o instanceof ResultSetTerm) {
                                    cnue = false;
                                } else {
                                    target.persist(o, s);
                                }
                            }
                        }
                    } catch (Exception e) {
                        if (e instanceof ExecutionException) {
                            logger.error("exception occured during cursor execution", e.getCause());
                        } else {
                            logger.error("exception occured during cursor execution", e);
                        }
                    }
                }

                done = ldone && rdone && prcrj;
                if (rdone && !prcrj) {
                    rtasks_.put(new FrameApiJoin());
                }
            }

            ret = current.getFrame(ldone && rdone);

            if (ret != null) {
                logger.debug("SQL cursor " + cur.getCursorId() + " returned next frame " + ret.getFrameId());
            }

            if (!target.isPersistent()) {
                target.persist(new ResultSetTerm(), s);
                logger.info("non-persistent cursor terminated");
            }
        } catch (Exception e) {
            logger.error("exception occured during cursor processing ", e);
        }

        return ret;
    }

    public synchronized boolean hasNextFrame() throws InternalException {
        bdnext = nextFrame2();
        return bdnext != null;
    }

    public synchronized void resetIterator() {

    }

    private List<SQLColumn> getIOTCList() {
        final List<SQLColumn> sl = cur.getSqlStmt().getCols().getColumns();
        final List<SQLColumn> ml = cur.getSqlStmt().getCols().getGroupColumns();
        final List<SQLColumn> res = new ArrayList<SQLColumn>();
        res.addAll(ml); //first, add index columns
        for (SQLColumn c : sl) {
            boolean chk = true;
            for (SQLColumn mc : ml) {
                if (mc.getId() == c.getId()) {
                    chk = false;
                    break;
                }
            }
            if (chk) res.add(c);
        }
        return res;
    }

    private Field getTargetColumn(SQLColumn c) throws NoSuchFieldException {
        final Table t = ((ResultSetImpl) target).getTarget();
        return t.getSc().getDeclaredField(c.getAlias());
    }

    public ResultSet flushTarget() throws InternalException {
        final boolean process = this.isProcess();
        if (!flush) {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    Thread.currentThread().setName("interference-sql-cursor-flush-thread-"+Thread.currentThread().getId());
                    try {
                        while (hasNextFrame()) {
                            nextFrame();
                        }

                        ((ResultSetImpl)target).release();

                        if (!target.isPersistent()) {
                            target.persist(new ResultSetTerm(), s);
                        }

                        if (process) {
                            logger.info("Process of cursor records complete");
                        }
                    } catch (Exception e) {
                        logger.error("Exception thrown during flush target operation", e);
                    }
                }
            };
            rspool.submit(r);
        }
        flush = true;
        return target;
    }

    public boolean isFlush() {
        return flush;
    }

    public int getId() {
        return id;
    }

    public FrameIterator getLbi() {
        return lbi;
    }

    public FrameIterator getRbi() {
        return rbi;
    }

    public FrameIterator getPbi() {
        return pbi;
    }

    public ResultSet getTarget() {
        return target;
    }

    public SQLJoinDispatcher getHmap() {
        return hmap;
    }

    public boolean isFurtherUseUC() {
        return furtherUseUC;
    }

    public SQLColumn getJoinedCC() {
        return joinedCC;
    }

    public SQLColumn getExtJoinedCC() {
        return extJoinedCC;
    }

    public void setExtJoinedCC(SQLColumn extJoinedCC) {
        this.extJoinedCC = extJoinedCC;
    }

    public List<Integer> getObjectIds() {
        return this.objectIds;
    }

    public boolean isLeftFS() {
        return leftFS;
    }

    @Override
    public boolean isLeftfs() {
        return false;
    }

    @Override
    public void setLeftfs(boolean leftfs) {

    }

    @Override
    public boolean noDistribute() {
        return true;
    }

    @Override
    public boolean isProcess() {
        return process;
    }

    @Override
    public Class getEventProcessor() {
        return evtprc;
    }

}

