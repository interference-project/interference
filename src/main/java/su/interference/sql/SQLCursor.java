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
    private List<FrameApiJoin> ltasks;
    private List<FrameApiJoin> rtasks;
    private List<FrameApiJoin> tasks_;
    private FrameData bdnext;
    private FrameHolder current;
    private int lptr = 0;
    private int rptr = 0;
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
    private final List<Integer> objectIds;
    private String rightType;
    private boolean leftFS;
    private final boolean furtherUseUC;
    private final SQLColumn joinedCC;
    private SQLColumn extJoinedCC;

    private static Map<Integer, Map<Long, ConcurrentLinkedQueue<FrameApi>>> sfmap = new ConcurrentHashMap<>();
    private static final int BATCH_SIZE = 8;
    private final static Logger logger = LoggerFactory.getLogger(SQLCursor.class);

    public SQLCursor (int id, FrameIterator lbi, FrameIterator rbi, NestedCondition nc, List<SQLColumn> rscols, boolean ixflag, boolean last, Cursor cur, Session s) throws Exception {
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

        ltasks = new ArrayList<>();
        rtasks = new ArrayList<>();
        tasks_ = new ArrayList<>();

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

        hmap = nc.getJoinDispatcher(lbi, rbi, this.rscols, s);
        //todo move to SQLJoinDispatcher
        if (hmap == null && rbi == null) {
            final ValueCondition vc = nc.getIndexVC(lbi, null);
            if (vc != null) {
                final Table lt = Instance.getInstance().getTableById(lbi.getObjectId());
                this.lbi = new SQLIndex(vc.getConditionColumn().getIndex(), lt, false, vc.getConditionColumn(), vc.getConditionColumn(), true, nc, 0, s);
                this.rbi = rbi;
            } else {
                this.lbi = lbi;
                this.rbi = rbi;
            }
        } else {
            this.lbi = hmap == null ? lbi : hmap.getLbi();
            this.rbi = hmap == null ? rbi : hmap.getRbi();
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
            logger.info("use full scan for "+lt.getName());
        }

        if (this.rbi != null) {
            final Table rt = Instance.getInstance().getTableById(this.rbi.getObjectId());
            if (this.rbi instanceof SQLTable) {
                logger.info("use full scan for "+rt.getName());
            }
            if (this.rbi instanceof SQLCursor) {
                logger.info("use full scan for "+rt.getName());
            }

        }
    }

    public int getType() {
        return FrameIterator.TYPE_CURSOR;
    }
    public boolean isIndex() throws MalformedURLException, ClassNotFoundException { return target.isIndex(); }

    public int getObjectId() {
        return target.getObjectId();
    }

    protected FrameJoinTask buildFrameJoinTask(int nodeId, FrameApi bd1, FrameApi bd2) {
        return new FrameJoinTask(cur, bd1, bd2, target, rscols, nc, id, nodeId, last, lbi.isLeftfs(), hmap, null, s);
    }

    public void build() throws Exception {
        final Integer[] ns = Config.getConfig().TEST_DISTRIBUTE_MODE == 0 ? TransportContext.getInstance().getOnlineNodesWithLocal() : TransportContext.getInstance().getNodesWithLocal();
        int i = 0;
        boolean isc = this.lbi instanceof SQLCursor || this.rbi instanceof SQLCursor;
        //int tnode = 0;

        while (lbi.hasNextFrame()) {
            final FrameApi bd1 = lbi.nextFrame();

            if (bd1 != null) {
                if (rbi == null) {
                    if (ns[i] == Config.getConfig().LOCAL_NODE_ID) {
                        ltasks.add(new FrameApiJoin(ns[i], this, bd1, null));
                    } else {
                        rtasks.add(new FrameApiJoin(ns[i], this, bd1, null));
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
                                ltasks.add(new FrameApiJoin(Config.getConfig().LOCAL_NODE_ID, this, bd1, bd2));
                            } else {
                                if (ns[i] == Config.getConfig().LOCAL_NODE_ID) {
                                    ltasks.add(new FrameApiJoin(ns[i], this, bd1, bd2));
                                } else {
                                    rtasks.add(new FrameApiJoin(ns[i], this, bd1, bd2));
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
        logger.info("SQL cursor is build: ltasks/rtasks amount = "+ltasks.size()+"/" + rtasks.size() + ", use NC check = " + last);
        if (!sent) {
            for (Integer nodeId : ns) {
                if (nodeId != Config.getConfig().LOCAL_NODE_ID) {
                    final Map<String, FrameApiJoin> joins = new HashMap<>();
                    for (FrameApiJoin j : rtasks) {
                        if (j.getNodeId() == nodeId) {
                            joins.put(j.getKey(), j);
                        }
                    }
                    final RemoteTask rt = new RemoteTask(cur, nodeId, joins, rightType, target.getTableClass() == null ? null : target.getTableClass().getName());
                    remotepool.submit(rt);
                }
            }
            sent = true;
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
                    final ConcurrentLinkedQueue<Object> q_in = new ConcurrentLinkedQueue<>();
                    FrameGroupTask group = null;
                    if (cur.getSqlStmt().isGroupedResult()) {
                        if (group == null) {
                            group = new FrameGroupTask(cur, q_in, target, gtable, s);
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
                        final FrameJoinTask task = new FrameJoinTask(cur, cf, null, target, ((StreamQueue) target).getRscols(), nc, id, Config.getConfig().LOCAL_NODE_ID, last, lbi.isLeftfs(), null, null, s);
                        final Future<List<Object>> ft = exec.submit(task);

                        if (cur.getSqlStmt().isGroupedResult()) {
                            q_in.addAll(ft.get());
                        } else {
                            for (Object o : ft.get()) {
                                target.persist(o, s);
                            }
                        }
                    }

                    while (((StreamQueue) target).isRunning()) {
                        FrameApi f = q.poll();
                        if (f != null) {
                            final FrameJoinTask task = new FrameJoinTask(cur, f, null, target, ((StreamQueue) target).getRscols(), nc, id, Config.getConfig().LOCAL_NODE_ID, last, lbi.isLeftfs(), null, null, s);
                            final Future<List<Object>> ft = exec.submit(task);

                            if (cur.getSqlStmt().isGroupedResult()) {
                                q_in.addAll(ft.get());
                            } else {
                                for (Object o : ft.get()) {
                                    target.persist(o, s);
                                }
                            }
                        }
                        if (q.peek() == null) {
                            Thread.sleep(100);
                        }
                    }
                } catch (Exception e) {
                    ((StreamQueue) target).stop();
                    e.printStackTrace();
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
        for (Map.Entry<Integer, Map<Long, ConcurrentLinkedQueue<FrameApi>>> entry : sfmap.entrySet()) {
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

    public Future<List<Object>> execute(FrameApi bd1, FrameApi bd2, FrameApiJoin j) {
        final FrameJoinTask task = new FrameJoinTask(cur, bd1, bd2, target, rscols, nc, id, Config.getConfig().LOCAL_NODE_ID, last, lbi.isLeftfs(), hmap, j, s);
        return exec.submit(task);
    }

    public synchronized FrameData nextFrame() {
        return bdnext;
    }

    private synchronized FrameData nextFrame2() throws InternalException {
        boolean ldone = lptr >= ltasks.size();
        boolean rdone = rptr >= rtasks.size();
        boolean done = ldone && rdone;
        FrameData ret = current.getFrame(ldone && rdone);

        while (!done && ret==null) {
            final ArrayList<Future<FrameApiJoin>> flist = new ArrayList<>();
            final ArrayList<Future<FrameApiJoin>> flist2 = new ArrayList<>();
            try {
                for (int i=0; i < BATCH_SIZE; i++) {
                    if (ldone) {
                        if (!rdone) {
                            if ((rptr + i) < rtasks.size()) {
                                final FrameApiJoin j = rtasks.get(rptr + i);
                                flist2.add(exec2.submit(j));
                            }
                        }
                    } else {
                        if ((lptr + i) < ltasks.size()) {
                            final FrameApiJoin j = ltasks.get(lptr + i);
                            flist.add(exec.submit(j));
                        }
                    }
                }
                if (ldone) {
                    if (!rdone) {
                        for (Future<FrameApiJoin> f : flist2) {
                            try {
                                final FrameApiJoin j = f.get();
                                if (j.isFailed()) {
                                    FrameApiJoin j_ = new FrameApiJoin(Config.getConfig().LOCAL_NODE_ID, this, j.getBd1(), j.getBd2());
                                    tasks_.add(j_);
                                } else {
                                    for (Object o : j.getResult()) {
                                        target.persist(o, s);
                                    }
                                }
                            } catch (Exception e) {
                                if (e instanceof ExecutionException) {
                                    e.getCause().printStackTrace();
                                } else {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                } else {
                    for (Future<FrameApiJoin> f : flist) {
                        try {
                            final FrameApiJoin j = f.get();
                            for (Object o : j.getResult()) {
                                target.persist(o, s);
                            }
                        } catch (Exception e) {
                            if (e instanceof ExecutionException) {
                                e.getCause().printStackTrace();
                            } else {
                                e.printStackTrace();
                            }
                        }
                    }
                }
                if (ldone) {
                    if (!rdone) {
                        rptr = rptr + BATCH_SIZE;
                    }
                } else {
                    lptr = lptr + BATCH_SIZE;
                }

                ldone = lptr >= ltasks.size();
                rdone = rptr >= rtasks.size();

                if (ldone && rdone && tasks_.size() > 0) {
                    ldone = false;
                    ltasks = tasks_;
                    tasks_ = new ArrayList<>();
                    lptr = 0;
                }

                done = ldone && rdone;
                ret = current.getFrame(ldone && rdone);
            } catch (Exception e) {
                e.printStackTrace();
            }

           logger.debug("SQL cursor "+cur.getCursorId()+" next frame returned frame "+(ret==null?"null":ret.getFrameId()));
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

                        if (!((ResultSetImpl)target).isPersistent()) {
                            target.persist(new ResultSetTerm(), s);
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

    public int getId() {
        return id;
    }

    public FrameIterator getLbi() {
        return lbi;
    }

    public FrameIterator getRbi() {
        return rbi;
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

}

