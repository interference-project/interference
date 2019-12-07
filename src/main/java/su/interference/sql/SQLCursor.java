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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.lang.reflect.InvocationTargetException;
import java.io.IOException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLCursor implements FrameIterator {

    private final int id;
    private ResultSet target;
    private boolean done;
    private ExecutorService exec = SQLJoinThreadPool.getThreadPool();
    private ArrayList<FrameJoinTask> tasks;
    private FrameData bdnext;
    private FrameHolder current;
    private int ptr = 0;
    private Cursor cur;
    private Session s;
    private ArrayList<SQLColumn> rscols;
    private NestedCondition nc;
    private boolean last;
    private boolean peristent;
    private final SQLJoinDispatcher hmap;
    private final FrameIterator lbi;
    private final FrameIterator rbi;
    private final List<Integer> objectIds;
    private boolean leftFS;
    private final boolean furtherUseUC;
    private final SQLColumn joinedCC;
    private SQLColumn extJoinedCC;

    private static final int BATCH_SIZE = 4;
    private final static Logger logger = LoggerFactory.getLogger(SQLCursor.class);

    public SQLCursor (int id, FrameIterator lbi, FrameIterator rbi, NestedCondition nc, ArrayList<SQLColumn> rscols, boolean ixflag, boolean last, Cursor cur, Session s) throws Exception {
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
        this.peristent = !cur.getSqlStmt().isEntityResult();

        //todo wrong case - column set must be rebuilded for prevent bad indexes intersect cases
        //todo need to refactor SQLJoin - extJoinedCC must be set on construct time

        if (cur.getType() == Cursor.SLAVE_TYPE && cur.getResultTargetName() != null && this.id == 1) {
            target = this.peristent ? s.registerTable(cur.getResultTargetName(), s, rscols, null, null, ixflag && last) : new ResultList(cur.getSqlStmt().getEntityTable());
        } else {
            target = this.peristent ? s.registerTable("su.interference.persistent.R$" + UUID.randomUUID().toString().replace('-', '$'), s, rscols, null, null, ixflag && last) : new ResultList(cur.getSqlStmt().getEntityTable());
        }
        current = new FrameHolder(target);

        tasks = new ArrayList<FrameJoinTask>();

        //rebuild column set for sqlcursor iterator
        final SQLCursor cursor_ = lbi.getType() == FrameIterator.TYPE_CURSOR ? (SQLCursor) lbi : rbi != null && rbi.getType() == FrameIterator.TYPE_CURSOR ? (SQLCursor) rbi : null;
        if (cursor_ != null) {
            ArrayList<SQLColumn> rscols_ = new ArrayList<>();
            for (SQLColumn sqlc : rscols) {
                if (cursor_.getObjectIds().contains(sqlc.getObjectId())) {
                    final Table t_ = (Table) cursor_.getTarget();
                    final SQLColumn sqlc_ = new SQLColumn(t_, sqlc.getId(), getTargetColumn(sqlc), sqlc.getAlias(), sqlc.getFtype(), sqlc.getLoc(), sqlc.getOrderOrd(), sqlc.getGroupOrd(), true, cursor_.isFurtherUseUC());
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
        if (hmap == null && rbi == null) {
            final ValueCondition vc = nc.getIndexVC(lbi, null);
            if (vc != null) {
                final Table lt = Instance.getInstance().getTableById(lbi.getObjectId());
                this.lbi = new SQLIndex(vc.getConditionColumn().getIndex(), lt, false, vc.getConditionColumn(), vc.getConditionColumn(), true, nc, s);
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

    public void build() throws InternalException, IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        final Integer[] ns = TransportContext.getInstance().getOnlineNodesWithLocal();
        int i = 0;
        //int tnode = 0;

        while (lbi.hasNextFrame()) {
            FrameApi bd1 = lbi.nextFrame();

            if (rbi == null) {
                tasks.add(new FrameJoinTask(cur, bd1, null, target, rscols, nc, id, ns[i], last, lbi.isLeftfs(), null, s));
                i++;
                if (i == ns.length) { i = 0; }
            } else {
                while (rbi.hasNextFrame()) {
                    FrameApi bd2 = rbi.nextFrame();
                    tasks.add(new FrameJoinTask(cur, bd1, bd2, target, rscols, nc, id, ns[i], last, lbi.isLeftfs(), hmap, s));
                    i++;
                    if (i == ns.length) { i = 0; }
                }
                rbi.resetIterator();
            }
        }
        logger.debug("SQL cursor is build: tasks amount = "+tasks.size()+", use NC check = "+last);
    }

    // execute single task (on local node only)
    public List<Object> execute(FrameApi bd1, FrameApi bd2) throws Exception {
        final FrameJoinTask task = new FrameJoinTask(cur, bd1, bd2, target, rscols, nc, id, Config.getConfig().LOCAL_NODE_ID, last, lbi.isLeftfs(), hmap, s);
        final Future<List<Object>> f = exec.submit(task);
        return f.get();
    }

    public synchronized FrameData nextFrame() {
        return bdnext;
    }

    private synchronized FrameData nextFrame2() throws InternalException {
        boolean done = !(ptr<tasks.size());
        FrameData ret = current.getFrame(done);

        while (!done&&ret==null) {
            try {
                ArrayList<Future<List<Object>>> flist = new ArrayList<Future<List<Object>>>();
                for (int i=0; i<BATCH_SIZE; i++) {
                    if ((ptr + i)<tasks.size()) {
                        FrameJoinTask jt = tasks.get(ptr + i);
                        flist.add(exec.submit(jt));
                    }
                }
                for (Future<List<Object>> f : flist) {
                    List<Object> ol = f.get();
                    logger.debug("SQL cursor next frame: the jointask call returned " + ol.size() + " records");
                    for (Object o : ol) {
                        target.persist(o, s);
                    }
                }
                ptr = ptr + BATCH_SIZE;
                done = !(ptr<tasks.size());
                ret = current.getFrame(done);
            } catch (Exception e) {
                e.printStackTrace();
            }

           logger.debug("SQL cursor "+cur.getCursorId()+" next frame returned frame "+(ret==null?"null":ret.getFrameId()));
        }
        return ret;
    }

    public synchronized boolean hasNextFrame() throws InternalException {
        bdnext = nextFrame2();
        if (bdnext!=null) {
            return true;
        }
        return false;
    }

    public synchronized void resetIterator() {
        
    }

    private Field getTargetColumn(SQLColumn c) throws NoSuchFieldException {
        final Table t = (Table)target;
        return t.getSc().getDeclaredField(c.getAlias());
    }

    public ResultSet flushTarget() throws InternalException {
        while (hasNextFrame()) {
            nextFrame();
        }
        return target;
    }

    public int getId() {
        return id;
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

}

