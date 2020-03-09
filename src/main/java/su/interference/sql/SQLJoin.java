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

import su.interference.metrics.Metrics;
import su.interference.persistent.*;
import su.interference.core.*;
import su.interference.sqlexception.SQLException;
import java.util.*;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLJoin {

    private final ArrayList<SQLTable> tables;
    private final CList columns;
    private final NestedCondition nc;
    private final Cursor cur;
    private int resultId;
    private List<SQLColumn> rscols;
    private ArrayList<SQLCursor> preparedCursors;
    private int targetId;
    private final List<SQLColumn> ocs;
    private final List<SQLColumn> gcs;
    private final List<SQLColumn> fcs;
    private ResultSet gtemp;

    public SQLJoin (ArrayList<SQLTable> tables, CList columns, NestedCondition nc, Cursor cur, Session s) throws Exception {
        this.tables   = tables;
        this.columns  = columns;
        this.nc = nc;
        this.cur = cur;
        this.ocs = this.columns.getOrderColumns();
        this.gcs = this.columns.getGroupColumns();
        this.fcs = this.columns.getFResultColumns();

        rscols = this.columns.getColumns();
        preparedCursors = new ArrayList<>();
        FrameIterator t1 = null;
        FrameIterator t2 = null;
        SQLTable tt1 = null;
        SQLTable tt2 = null;

        if (gcs.size()>0) {
            rscols = this.getIOTCList(gcs, this.columns.getColumns());
        } else {
            if (ocs.size()>0) {
                rscols = this.getIOTCList(ocs, this.columns.getColumns());
            }
        }

        int i = 0;
        boolean ixflag = ocs.size()==0&&gcs.size()==0?false:true;
        for (SQLTable sqlt : this.tables) {
            if (t1==null) {
                t1 = sqlt;
                tt1 = sqlt;
                if (this.tables.size()==1) {
                    //todo last = true always?
                    SQLCursor sqlcur = new SQLCursor(1, t1, null, nc, rscols, ixflag, true, this.cur, s);
                    preparedCursors.add(sqlcur);
                    this.targetId = sqlcur.getTarget().getObjectId();
                }
                continue;
            }
            i++;
            t2 = sqlt;
            tt2 = sqlt;
            // ixflag cleans inside SQLCursor if !last
            // todo ? generate index by extJoinedColumn?
            SQLCursor sqlcur = new SQLCursor(i, t1, t2, nc, rscols, ixflag, i==this.tables.size()-1, this.cur, s);
            if (preparedCursors.size()>0) {
                preparedCursors.get(preparedCursors.size()-1).setExtJoinedCC(sqlcur.getJoinedCC());
            }
            nc.coordinateConditions(tt1, tt2, i);
            t1 = sqlcur;
            tt1 = tt2;
            preparedCursors.add(sqlcur);
            if (i==this.tables.size()-1) {
                this.targetId = sqlcur.getTarget().getObjectId();
            }
        }
        this.cur.setState(Cursor.STATE_PREPARED);
    }

    //mode = 0 for select (create temporary objects)
    //mode = 1 for update (don't create temporary objects and update DR)
    //mode = 2 for delete (don't create temporary objects and delete DR)
    //uset is update set of values (use in mode 1 only)
    public ResultSet executeJoin(Session s, int mode, ArrayList<SQLSetValue> uset) throws Exception {

        this.cur.setState(Cursor.STATE_RUNNING);

        ResultSet temp;

        if (mode==0) { //pessimistic check

            SQLCursor last = null;
            for (SQLCursor sqlc : preparedCursors) {
                last = sqlc;
                if (cur.isStream()) {
                    sqlc.stream();
                } else {
                    sqlc.build();
                }
            }

            temp = last.getTarget();

            this.cur.setState(Cursor.STATE_COMPLETED);

        } else {
            throw new SQLException("Illegal join mode for multitable statement");
        }

        //group processing

        if (this.cur.getType()==Cursor.MASTER_TYPE) {
            if (gcs.size()>0||fcs.size()>0) {
                boolean ixflag = ocs.size() > 0 ? true : false;

                //todo getall
                gtemp = new ResultSetImpl(s.registerTable("su.interference.persistent.G$" + UUID.randomUUID().toString().replace('-', '$'), s, rscols, null, null, ixflag), null, false);
                final Table gtarget = ((ResultSetImpl)gtemp).getTarget();
                //warning! use rscols set for both 1st table (ordered by group columns) and 2nd (group records)
                //warning! use 2 tables with SAME value sets - use dc.getEntity(Table) method
                DataChunk cdc = null;
                SQLGroup sqlg = null;
                boolean cnue = true;

                while (cnue) {
                    Chunk c = temp.cpoll(s);
                    if (c == null) {
                        cnue = false;
                    } else {
                        if (cdc != null) {
                            if (((DataChunk) c).compare(cdc, gcs.size()) == 0) { //cdc & c chunks grouped
                                sqlg.add((DataChunk) c);
                            } else {                                          //c start next group
                                DataChunk gdc = sqlg.getDC();
                                Object oo = gdc.getEntity(gtarget);
                                gtemp.persist(oo, s);
                                sqlg = new SQLGroup((DataChunk) c, rscols);
                                sqlg.add((DataChunk) c);
                            }
                        } else {
                            sqlg = new SQLGroup((DataChunk) c, rscols);
                            sqlg.add((DataChunk) c);
                        }
                        cdc = (DataChunk) c;
                    }
                }
                if (sqlg != null) {
                    DataChunk gdc = sqlg.getDC();
                    Object oo = gdc.getEntity(((ResultSetImpl)gtemp).getTarget());

                    gtemp.persist(oo, s);

                }
                if (!((ResultSetImpl)gtemp).isPersistent()) {
                    gtemp.persist(new ResultSetTerm(), s);
                }

                ((ResultSetImpl)gtemp).release();
            }
        }

        if (mode==0) {
            this.resultId = temp.getObjectId();
        }    

        return gtemp==null?temp:gtemp;
    }

    protected SQLCursor getSQLCursorById(int id) {
        for (SQLCursor c : preparedCursors) {
            if (c.getId() == id) {
                return c;
            }
        }
        return null;
    }

    public ArrayList<ValueCondition> getValueConditions(int objectId) {
        final ArrayList<ValueCondition> res = new ArrayList<ValueCondition>();
        for (Condition c : this.nc.getValueConditions()) {
            if (c.getConditionColumn().getObjectId()==objectId) {
                res.add((ValueCondition)c);
            }
        }
        return res;
    }

    public List<SQLColumn> getIOTCList(List<SQLColumn> ml, List<SQLColumn> sl) {
        final List<SQLColumn> res = new ArrayList<>();
        res.addAll(ml); //first, add index columns
        for (SQLColumn c : sl) {
            boolean chk = true;
            for (SQLColumn mc : ml) {
                if (mc.getId()==c.getId()) {
                    chk = false;
                }
            }
            if (chk) res.add(c);
        }
        return res;
    }

    public String getResultTargetName() {
        return ((ResultSetImpl)preparedCursors.get(preparedCursors.size()-1).getTarget()).getTarget().getName();
    }

    public void deallocate(Session s) throws Exception {
        Metrics.get("deallocateQuery").start();
        for (SQLCursor c : preparedCursors) {
            c.getTarget().deallocate(s);
            s.delete(c.getTarget());
        }
        if (gtemp != null) {
            gtemp.deallocate(s);
            s.delete(gtemp);
        }
        Metrics.get("deallocateQuery").stop();
    }

    public int getTargetId() {
        return targetId;
    }

    public ArrayList<SQLTable> getTables() {
        return tables;
    }

    public CList getColumns() {
        return columns;
    }

}
