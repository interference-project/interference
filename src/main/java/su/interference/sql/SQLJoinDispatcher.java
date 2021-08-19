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
import su.interference.core.Instance;
import su.interference.exception.InternalException;
import su.interference.persistent.Session;
import su.interference.persistent.Table;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLJoinDispatcher implements Comparable {

    private final Table ix1;
    private final Table ix2;
    private final FrameIterator lbi;
    private final FrameIterator rbi;
    private final ValueCondition vc1;
    private final ValueCondition vc2;
    private final boolean merged;
    private final boolean skipCheckNC;
    private final boolean furtherUseUC;
    private final SQLColumn joinedCC;
    private final int weight;
    private final int join;
    private final Session s;
    private final static Logger logger = LoggerFactory.getLogger(SQLJoinDispatcher.class);
    public final static int MERGE = 1;
    public final static int RIGHT_MERGE = 2;
    public final static int RIGHT_HASH = 3;
    public final static int RIGHT_INDEX = 4;
    public final static int NESTED_LOOPS = 10;

    public SQLJoinDispatcher(FrameIterator lbi, FrameIterator rbi, SQLColumn c1, SQLColumn c2, boolean skip, NestedCondition nc, Session s)
            throws IOException, ClassNotFoundException, InternalException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

        //depends on join columns is unique or index, standard iterators (Table, Cursor) must replace by additional
        //iterators - HashMap and Index. HashMap may be only RBI, Index may be LBI in case if both indexes exists
        vc1 = nc.getIndexVC(lbi, null);
        vc2 = nc.getIndexVC(rbi, null);
        //value conditions with EQUAL has high priority for use indexes than current merge join
        merged = vc1 == null && vc2 == null;
        ix1 = vc1 == null ? c1.getIndex() : vc1.getCondition() == Condition.C_EQUAL || vc1.getCondition() == Condition.C_IN ? vc1.getConditionColumn().getIndex() : c1.getIndex();
        ix2 = vc2 == null ? c2.getIndex() : vc2.getCondition() == Condition.C_EQUAL || vc1.getCondition() == Condition.C_IN ? vc2.getConditionColumn().getIndex() : c2.getIndex();
        FrameIterator lbi_ = null;
        FrameIterator rbi_ = null;

        //deprecated
        this.joinedCC = lbi.getType() == FrameIterator.TYPE_CURSOR?c1:c2;

        if (c1.isIndexOrUnique()||c2.isIndexOrUnique()) {
            if (c1.isUnique() || c2.isUnique()) {
                this.join = RIGHT_HASH;
                final Table lt = Instance.getInstance().getTableById(lbi.getObjectId());
                final Table rt = Instance.getInstance().getTableById(rbi.getObjectId());
                FrameIterator hbi = null;
                if (c1.isUnique()) {
                    lbi_ = ix2 == null ? rbi : new SQLIndex(ix2, rt, true, c1, c2, false, nc, RIGHT_HASH, s);
                    hbi = ix1 == null ? lbi : new SQLIndex(ix1, lt, false, c1, c2, false, nc, RIGHT_HASH, s);
                } else if (c2.isUnique()) {
                    lbi_ = ix1 == null ? lbi : new SQLIndex(ix1, lt, true, c1, c2, false, nc, RIGHT_HASH, s);
                    hbi = ix2 == null ? rbi : new SQLIndex(ix2, rt, false, c1, c2, false, nc, RIGHT_HASH, s);
                }
                final Table lt_ = Instance.getInstance().getTableById(lbi_.getObjectId());
                final Table rt_ = Instance.getInstance().getTableById(hbi.getObjectId());
                SQLColumn cmap_ = hbi.getObjectId() == c1.getObjectId() ? c1 : c2;
                SQLColumn ckey_ = hbi.getObjectId() == c1.getObjectId() ? c2 : c1;
                logger.info("use right hash join for " + lt_.getName() + "." + c1.getColumn().getName() + " * " + rt_.getName() + "." + c2.getColumn().getName());
                rbi_ = new SQLHashMap(cmap_, ckey_, hbi, rt_, s);
                this.weight = 60;
            } else if (c1.isUnique() && merged && ix1 != null && ix2 != null) {
                this.join = MERGE;
                final Table lt = Instance.getInstance().getTableById(lbi.getObjectId());
                final Table rt = Instance.getInstance().getTableById(rbi.getObjectId());
                logger.info("use merge join for " + lt.getName() + "." + c1.getColumn().getName() + " * " + rt.getName() + "." + c2.getColumn().getName());
                lbi_ = new SQLIndex(ix1, lt, true, c1, c2, true, nc, MERGE, s);
                rbi_ = new SQLIndex(ix2, rt, false, c1, c2, true, nc, MERGE, s);
                this.weight = 100;
            } else if (c2.isUnique() && merged && ix1 != null && ix2 != null) {
                this.join = MERGE;
                final Table lt = Instance.getInstance().getTableById(rbi.getObjectId());
                final Table rt = Instance.getInstance().getTableById(lbi.getObjectId());
                logger.info("use merge join for " + lt.getName() + "." + c2.getColumn().getName() + " * " + rt.getName() + "." + c1.getColumn().getName());
                lbi_ = new SQLIndex(ix2, lt, true, c2, c1, true, nc, MERGE, s);
                rbi_ = new SQLIndex(ix1, rt, false, c2, c1, true, nc, MERGE, s);
                this.weight = 100;
            } else {
                this.join = RIGHT_INDEX;
                final Table lt = Instance.getInstance().getTableById(lbi.getObjectId());
                final Table rt = Instance.getInstance().getTableById(rbi.getObjectId());
                logger.info("use index scan for " + lt.getName() + "." + c1.getColumn().getName() + " * " + rt.getName() + "." + c2.getColumn().getName());
                if (ix1 != null) {
                    lbi_ = rbi;
                    rbi_ = new SQLIndex(ix1, lt, false, c2, c1, false, nc, RIGHT_INDEX, s);
                } else if (ix2 != null) {
                    lbi_ = lbi;
                    rbi_ = new SQLIndex(ix2, rt, false, c1, c2, false, nc, RIGHT_INDEX, s);
                }
                this.weight = 30;
            }
        } else {
            final Table lt = Instance.getInstance().getTableById(lbi.getObjectId());
            final Table rt = Instance.getInstance().getTableById(rbi.getObjectId());
            logger.info("use nested loops for " + lt.getName() + "." + c1.getColumn().getName() + " * " + rt.getName() + "." + c2.getColumn().getName());
            lbi_ = lbi;
            rbi_ = rbi;
            this.join = NESTED_LOOPS;
            this.weight = 0;
        }
        this.lbi = lbi_;
        this.rbi = rbi_;
        this.skipCheckNC = skip;
        this.furtherUseUC = c1.isUnique() && c2.isUnique();
        this.s = s;
    }

    protected FrameIterator getLbi() {
        return lbi;
    }

    protected FrameIterator getRbi() {
        return rbi;
    }

    public boolean skipCheckNC() {
        return skipCheckNC;
    }

    public boolean isFurtherUseUC() {
        return furtherUseUC;
    }

    public SQLColumn getJoinedCC() {
        return joinedCC;
    }

    //desc sorting
    public int compareTo(Object obj) {
        SQLJoinDispatcher t = (SQLJoinDispatcher)obj;
        if (this.getWeight() > t.getWeight()) {
            return -1;
        } else if (this.getWeight() < t.getWeight()) {
            return 1;
        }
        return 0;
    }

    public int getJoin() {
        return join;
    }

    public int getWeight() {
        return weight;
    }
}
