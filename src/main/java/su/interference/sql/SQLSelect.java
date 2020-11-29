/**
 The MIT License (MIT)

 Copyright (c) 2010-2020 head systems, ltd

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
import su.interference.metrics.Metrics;
import su.interference.persistent.*;
import su.interference.sqlexception.*;
import su.interference.core.DataSet;
import su.interference.transport.*;

import java.util.*;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLSelect implements SQLStatement {
    private static final String SELECT_CLAUSE = "SELECT";
    private static final String STREAM_CLAUSE = " STREAM ";
    private static final String DISTINCT_CLAUSE = " DISTINCT ";
    private static final String FROM_CLAUSE = " FROM ";
    private static final String WHERE_CLAUSE = " WHERE ";
    private static final String ORDERBY_CLAUSE = " ORDER BY ";
    private static final String GROUPBY_CLAUSE = " GROUP BY ";
    private static final String HAVING_CLAUSE = " HAVING ";
    private static final String WINDOWBY_CLAUSE = " WINDOW BY ";
    public static final String INTERVAL_CLAUSE = "INTERVAL";

    private final static Logger logger = LoggerFactory.getLogger(SQLSelect.class);
    private Cursor              cursor;
    private ArrayList<SQLTable> tables; //possibly contains INLINE SQLSelects
    private CList               cols;   //set by query
    private NestedCondition     nc;
    private SQLJoin             join;
    private ArrayList<SQLNode>  nodes;

    private boolean stream;
    private boolean distinct;
    private boolean entityResult;
    private Table   entityTable;
    private SQLException sqlException;

    public SQLException getSQLException() {
        return sqlException;
    }

    public SQLSelect () {
        
    }

    public SQLSelect (String sql, Session s) {
        this(sql, null, s);
    }

    public SQLSelect (String sql, Cursor cur, Session s) {
        tables = new ArrayList<SQLTable>();
        nodes = new ArrayList<SQLNode>();
        cols = null; //until parse tables part (FROM...) cols must = null
        try {
            parseSQL(sql, cur, s);
        } catch (SQLException e) {
            sqlException = e;
            logger.error(e.getClass().getSimpleName()+" thrown during parse of sql statement: "+sql);
        } catch (Exception e) {
            sqlException = new SQLException(e.getMessage());
            logger.error(e.getClass().getSimpleName()+" thrown during parse of sql statement: "+sql);
            e.printStackTrace();
        }
    }

    public void checkSQL () {
        for (SQLTable sqlt : this.tables) {
            logger.debug("table: "+sqlt.getTable().getName()+" with alias "+sqlt.getAlias());
        }
        for (SQLColumn sqlc : this.cols.getColumns()) {
            logger.debug("column: "+sqlc.getColumn().getName()+" with alias "+sqlc.getAlias());
        }
        if (nc!=null) {
            for (Condition c : nc.getAllConditions()) {
                if (c.getClass().getName().equals("su.interference.sql.ValueCondition")) {
                    ValueCondition vc = (ValueCondition)c;
                    logger.debug("condition: VC type "+c.getCondition()+" with column "+c.getConditionColumn().getColumn().getName()+" values:");
                    if (vc.getValues()==null) {
                        logger.debug("values = null");
                    } else {
                        if (vc.getValues().length==0) {
                            logger.debug("values.len = 0");
                        } else {
                            for (Object s : vc.getValues()) {
                                logger.debug(String.valueOf(s));
                            }
                        }
                    }
                }
                if (c.getClass().getName().equals("su.interference.sql.JoinCondition")) {
                    JoinCondition jc = (JoinCondition)c;
                    logger.debug("condition: JC type "+jc.getCondition()+" with columns "+jc.getConditionColumn().getColumn().getName()+","+jc.getConditionColumnRight().getColumn().getName());
                }
                if (c.getClass().getName().equals("su.interference.sql.NestedCondition")) {
                    NestedCondition nc = (NestedCondition)c;
                    logger.debug("condition: NC join type "+nc.getType()+" contains "+nc.getConditions().size()+" conditions");
                }
            }
        }
    }

    public DataSet executeSQL (Session s) {
        String message = ""; // debug information
        ResultSet t = null;
        Metrics.get("executeQuery").start();

        try {
            t = this.join.executeJoin(s,0,null); //mode=0 for select, uset is null
            if (!stream) {
                logger.info("sql executed");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        ArrayList<Object> result = new ArrayList<Object>();
        Metrics.get("executeQuery").stop();
        return new DataSet(result.toArray(new Object[]{}), t, message);
    }

    @SuppressWarnings("unchecked")
    private final void parseSQL (String s, Cursor cur, Session sn) throws Exception {

        final String sql = s.trim();
        final String SQL = s.toUpperCase().trim();

        if (!SQL.startsWith(SELECT_CLAUSE)) {
            throw new InvalidSQLStatement();
        }
        if (SQL.indexOf(STREAM_CLAUSE) == SELECT_CLAUSE.length()) {
            this.stream = true;
        }
        if (SQL.indexOf(DISTINCT_CLAUSE) == SELECT_CLAUSE.length()) {
            this.distinct = true;
        }
        if (SQL.indexOf(FROM_CLAUSE) < SELECT_CLAUSE.length() + 1) {
            throw new MissingFromClause();
        }
        if (sql.substring(SELECT_CLAUSE.length(), SQL.indexOf(" FROM ")).trim().equals("")) {
            throw new MissingFromClause();
        }

        cursor = cur==null?new Cursor(sql, this.stream ? Cursor.STREAM_TYPE : Cursor.MASTER_TYPE):cur;
        cursor.setSqlStmt(this);

        int baselen = sql.length();

        if (SQL.indexOf(WHERE_CLAUSE)>=0) {
            baselen = SQL.indexOf(WHERE_CLAUSE);
        }
        if (SQL.indexOf(ORDERBY_CLAUSE)>=0 && baselen>SQL.indexOf(ORDERBY_CLAUSE)) {
            baselen = SQL.indexOf(ORDERBY_CLAUSE);
        }
        if (SQL.indexOf(GROUPBY_CLAUSE)>=0 && baselen>SQL.indexOf(GROUPBY_CLAUSE)) {
            baselen = SQL.indexOf(GROUPBY_CLAUSE);
        }
        if (SQL.indexOf(WINDOWBY_CLAUSE)>=0 && baselen>SQL.indexOf(WINDOWBY_CLAUSE)) {
            baselen = SQL.indexOf(WINDOWBY_CLAUSE);
        }
        if (baselen < SQL.indexOf(FROM_CLAUSE) + 6) {
            throw new MissingTablesDescription();
        } else {
            if (sql.substring(SQL.indexOf(FROM_CLAUSE) + 6, baselen).trim().equals("")) {
                throw new MissingTablesDescription();
            }
        }

        final int cldsStart = stream ? SELECT_CLAUSE.length() + STREAM_CLAUSE.length() :
                              distinct ? SELECT_CLAUSE.length() + DISTINCT_CLAUSE.length() :
                              SELECT_CLAUSE.length();
        final String[] clds = sql.substring(cldsStart, SQL.indexOf(FROM_CLAUSE)).trim().split(",");
        final String[] tbls = sql.substring(SQL.indexOf(FROM_CLAUSE)+6, baselen).trim().split(",");

        final int wpos = SQL.indexOf(WHERE_CLAUSE);
        final int opos = SQL.indexOf(ORDERBY_CLAUSE);
        final int gpos = SQL.indexOf(GROUPBY_CLAUSE);
        final int xpos = SQL.indexOf(WINDOWBY_CLAUSE);

        if (wpos>=0 && opos>=0) {
            if (wpos > opos) {
                throw new InvalidSQLStatement();
            }
        }
        if (wpos>=0 && gpos>=0) {
            if (wpos > gpos) {
                throw new InvalidSQLStatement();
            }
        }
        if (wpos>=0 && xpos>=0) {
            if (wpos > xpos) {
                throw new InvalidSQLStatement();
            }
        }
        if (opos>=0 && gpos>=0) {
            if (opos < gpos) {
                throw new InvalidSQLStatement();
            }
        }
        if (opos>=0 || gpos>=0) {
            if (xpos > 0) {
                throw new InvalidSQLStatement();
            }
        }

        baselen = sql.length();

        if (SQL.indexOf(ORDERBY_CLAUSE)>=0 && baselen>SQL.indexOf(ORDERBY_CLAUSE)) {
            baselen = SQL.indexOf(ORDERBY_CLAUSE);
        }
        if (SQL.indexOf(GROUPBY_CLAUSE)>=0 && baselen>SQL.indexOf(GROUPBY_CLAUSE)) {
            baselen = SQL.indexOf(GROUPBY_CLAUSE);
        }
        if (SQL.indexOf(WINDOWBY_CLAUSE)>=0 && baselen>SQL.indexOf(WINDOWBY_CLAUSE)) {
            baselen = SQL.indexOf(WINDOWBY_CLAUSE);
        }

        // parsing tables
        for (int i=0; i<tbls.length; i++) {
            String[] tblss = tbls[i].trim().split(" ");
            if (tblss.length==1) { //without alias
                this.tables.add(new SQLTable(tblss[0],tblss[0]));
            }
            if (tblss.length==2) { //with alias
                this.tables.add(new SQLTable(tblss[0],tblss[1]));
            }
        }

        Collections.sort(this.tables);

        this.cols = new CList (this.tables, clds);

        this.nc = new NestedCondition(wpos>=0?sql.substring(wpos+7, baselen).trim():"",this,null);

        //check for entity result
        if (clds.length==1) {
            if (clds[0].trim().equals("*")) {
                if (this.tables.size() == 1) {
                    this.entityResult = true;
                    this.entityTable = tables.get(0).getTable();
                } else {
                    throw new InvalidColumnDescription();
                }
            }
        }

        baselen = sql.length();

        if ((SQL.indexOf(GROUPBY_CLAUSE)>=0)&&(baselen>SQL.indexOf(GROUPBY_CLAUSE))) {
            baselen = SQL.indexOf(GROUPBY_CLAUSE);
        }

        final String ord = opos>=0?sql.substring(opos+10, baselen):"";
        final String grd = gpos>=0?sql.substring(gpos+10):"";
        final String wnd = xpos>=0?sql.substring(xpos+11):"";

        if (!ord.trim().equals("")) {
            String[] ords = ord.trim().split(",");
            int ord_ = 1;
            for (String ordr : ords) {
                try {
                    cols.check(ordr, ordr.trim(), CList.LOC_ORDER, ord_);
                    ord_++;
                } catch (SQLException e) {
                    throw new InvalidOrderByPart();
                }
            }
        }

        if (!grd.trim().equals("")) {
            String[] grds = grd.trim().split(",");
            int ord_ = 1;
            for (String grp : grds) {
                try {
                    cols.check(grp.trim(), grp.trim(), CList.LOC_GROUP, ord_);
                    ord_++;
                } catch (SQLException e) {
                    throw new InvalidGroupByPart();
                }
            }
        }

        if (!wnd.trim().equals("")) {
            try {
                cols.check(wnd.trim(), wnd.trim(), CList.LOC_WINDOW, 1);
            } catch (SQLException e) {
                throw new InvalidGroupByPart();
            }
        }

        //group checks
        final List<SQLColumn> rscl = this.cols.getNoFResultColumns();
        final List<SQLColumn> frcl = this.cols.getFResultColumns();
        final List<SQLColumn> grcl = this.cols.getGroupColumns();

        //check group functions
        if (frcl.size()==0 && grcl.size()>0) {
            throw new InvalidGroupColumnSet();
        }

        //check group functions
        if (frcl.size()>0 && grcl.size()==0) {
            if (rscl.size() > 0) {
                throw new InvalidGroupColumnSet();
            }
        }

        //check intersect of args and group column sets
        for (SQLColumn grc : grcl) {
            boolean chk = false;
            for (SQLColumn rsc : rscl) {
                if (rsc.getId() == grc.getId()) {
                    chk = true;
                    break;
                }
            }
            if (!chk) {
                throw new InvalidGroupColumnSet();
            }
        }

        cols.sort();

        sn.persist(this.cursor);
        this.join = new SQLJoin(this.tables, this.cols, this.nc, this.cursor, sn);
        sn.getTransaction().setJoin(this.join);

        // try to start distribute query
        if (this.cursor.getType() == Cursor.MASTER_TYPE) {

            this.cursor.setTargetId(this.join.getTargetId());

            final Integer[] ns = TransportContext.getInstance().getOnlineNodes();
            for (Integer nodeId : TransportContext.getInstance().getOnlineNodes()) {
                final TransportEvent transportEvent = new SQLEvent(nodeId, cursor.getCursorId(), null, null,
                        this.join.getTargetId(), cursor.getSql(), this.join.getResultTargetNames(), sn.getTransaction().getTransId(), false);
                TransportContext.getInstance().send(transportEvent);
                logger.debug("create remote cursor on node " + nodeId);
                transportEvent.getLatch().await();
                if (!transportEvent.isFail()) {
                    nodes.add(new SQLNode(nodeId, SQLNode.NODE_TYPE_SLAVE, ((SQLEvent) transportEvent).getCallback().getResult().getSlaveCursorid()));
                }
            }
        }
    }

    public SQLCursor getSQLCursorById(int id) {
        return this.join.getSQLCursorById(id);
    }

    public ArrayList<SQLTable> getTables() {
        return tables;
    }

    public void setTables(ArrayList<SQLTable> tables) {
        this.tables = tables;
    }

    public CList getCols() {
        return cols;
    }

    public NestedCondition getNc() {
        return nc;
    }

    public void setNc(NestedCondition nc) {
        this.nc = nc;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public boolean isEntityResult() {
        return entityResult;
    }

    public void setEntityResult(boolean entityResult) {
        this.entityResult = entityResult;
    }

    public boolean isGroupedResult() {
        return cols.getGroupColumns().size() > 0 || cols.getFResultColumns().size() > 0;
    }

    public Table getEntityTable() {
        return entityTable;
    }

    public void setEntityTable(Table entityTable) {
        this.entityTable = entityTable;
    }
}
