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

import su.interference.persistent.Table;
import su.interference.sqlexception.InvalidWindowByPart;
import su.interference.sqlexception.MissingTablesDescription;
import su.interference.sqlexception.AmbiguousColumnName;
import su.interference.sqlexception.InvalidColumnDescription;
import su.interference.exception.InternalException;

import java.util.ArrayList;
import java.util.Collections;
import java.lang.reflect.Field;
import java.util.List;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class CList {
    public static final int LOC_RESULT = 1;
    public static final int LOC_WHERE = 2;
    public static final int LOC_ORDER = 3;
    public static final int LOC_GROUP = 4;
    public static final int LOC_WINDOW = 5;

    private final ArrayList<SQLTable> tables;
    private final ArrayList<SQLColumn> columns;

    public CList (ArrayList<SQLTable> sqlt, String[] clds) throws Exception {
        if (sqlt == null) { throw new MissingTablesDescription(); }
        this.tables = sqlt;
        this.columns = new ArrayList<>();
        if (clds.length == 1 && clds[0].trim().equals("*")) {
            if (sqlt.size() == 1) {
                SQLTable st = sqlt.get(0);
                final Field[] cs = st.getTable().getFields();
                for (int j=0; j<cs.length; j++) {
                    check(cs[j].getName(), cs[j].getName(), CList.LOC_RESULT, 0);
                }
            } else {
                throw new InternalException();
            }
        } else {
            for (String cld : clds) {
                String[] cld_ = cld.trim().split(" ");
                if (cld_.length == 1) { //without alias
                    check(cld_[0], cld_[0], CList.LOC_RESULT, 0);
                }
                if (cld_.length == 2) { //with alias
                    check(cld_[0], cld_[1], CList.LOC_RESULT, 0);
                }
            }
        }
    }

    protected final SQLColumn check (String c, String alias, int loc, int ord) throws Exception {
        //first, check functions
        final int cf = SQLColumn.checkFunction(c);
        String column  = null;
        Field dbcolumn = null;
        int dbcolumnId = 0;
        Table dbtable = null;
        int windowInterval = 0;

        if (cf>0 && c.substring(c.length()-1,c.length()).equals(")")) {
            column = c.trim().substring(c.indexOf("(")+1,c.length()-1);
            if (c.equals(alias)) {
                alias = c.trim().substring(0,c.indexOf("("))+c.trim().substring(c.indexOf("(")+1,c.length()-1);
            }
        } else {
            if (loc == LOC_WINDOW) {
                if (c.toUpperCase().indexOf(SQLSelect.INTERVAL_CLAUSE)<=0) {
                    throw new InvalidWindowByPart();
                }
                String[] intd = c.substring(c.toUpperCase().indexOf(SQLSelect.INTERVAL_CLAUSE)+SQLSelect.INTERVAL_CLAUSE.length()).split("=");
                if (intd.length != 2) {
                    throw new InvalidWindowByPart();
                }
                try {
                    windowInterval = Integer.valueOf(intd[1].trim());
                } catch (Exception e) {
                    throw new InvalidWindowByPart();
                }
                column = c.trim().substring(0,c.toUpperCase().indexOf(SQLSelect.INTERVAL_CLAUSE)).trim();
                alias = column;
            } else {
                column = c.trim();
            }
        }

        String[] cldss = column.trim().split("\\.");
        if (cldss.length==1) { //column without prefix
            for (SQLTable st : this.tables) {
                int cnt = 0;
                final Field[] cs = st.getTable().getFields();
                for (int j=0; j<cs.length; j++) {
                    if (cs[j].getName().trim().equals(column.trim())) {
                        if (cnt>0) {
                            throw new AmbiguousColumnName();
                        }
                        dbcolumn = cs[j];
                        dbcolumnId = st.getTable().getObjectId()*1000 + j;
                        dbtable = st.getTable();
                        cnt = 1;
                    }
                }
            }
        }

        if (cldss.length==2) { //column with prefix
            for (SQLTable st : this.tables) {
                if (st.getAlias().trim().equals(cldss[0].trim())) {
                    final Field[] cs = st.getTable().getFields();
                    for (int j=0; j<cs.length; j++) {
                        if (cs[j].getName().trim().equals(cldss[1].trim())) {
                            dbcolumn = cs[j];
                            dbcolumnId = st.getTable().getObjectId()*1000 + j;
                            dbtable = st.getTable();
                        }
                    }
                }
            }
        }

        if (loc!=LOC_WHERE) { //where conditions may contains values, method simply returns null
            if (dbcolumn == null) {
                throw new InvalidColumnDescription();
            }
        }

        if (dbcolumn!=null) {
            //check for column already exists
            boolean exists = false;
            for (SQLColumn cl : this.columns) {
                if (cl.getId()==dbcolumnId) {
                    exists = true;
                    if (loc==LOC_RESULT) { cl.setResult (true); }
                    if (loc==LOC_WHERE) { cl.setWhere (true); }
                    if (loc==LOC_ORDER) { cl.setOrder (true); cl.setOrderOrd(ord); }
                    if (loc==LOC_GROUP) { cl.setGroup (true); cl.setGroupOrd(ord); }
                    if (loc==LOC_WINDOW)  {
                        cl.setWindow(true);
                        cl.setWindowInterval(windowInterval);
                    }
                    return cl;
                }
            }
            if (!exists) {
                String[] aldss = alias.trim().split("\\.");
                String al = alias;
                if (aldss.length==2) { //remove . from alias
                    al = aldss[0]+aldss[1];
                }

                SQLColumn sqlc = new SQLColumn(dbtable, dbcolumnId, dbcolumn, al, cf, loc, 0, 0, windowInterval, false, true);

                this.columns.add(sqlc);
                return sqlc;
            }
        }

        return null;
    }

    public void sort () {
        Collections.sort(this.columns);
    }

    public List<SQLColumn> getColumns() {
        return columns;
    }

    public List<Field> getResultColumns() {
        final List<Field> res = new ArrayList<Field>();
        for (SQLColumn sqlc : this.columns) {
            if (sqlc.isResult()) {
                res.add(sqlc.getColumn());
            }
        }
        return res;
    }

    public List<SQLColumn> getFResultColumns() {
        final List<SQLColumn> res = new ArrayList<SQLColumn>();
        for (SQLColumn sqlc : this.columns) {
            if (sqlc.isResult()&&sqlc.getFtype()>0) {
                res.add(sqlc);
            }
        }
        return res;
    }

    public List<SQLColumn> getNoFResultColumns() {
        final List<SQLColumn> res = new ArrayList<SQLColumn>();
        for (SQLColumn sqlc : this.columns) {
            if (sqlc.isResult()&&sqlc.getFtype()==0) {
                res.add(sqlc);
            }
        }
        return res;
    }

    public List<SQLColumn> getOrderColumns() {
        final List<SQLColumn> res = new ArrayList<SQLColumn>();
        for (SQLColumn sqlc : this.columns) {
            if (sqlc.isOrder()) {
                res.add(sqlc);
            }
        }
        Collections.sort(res, new ColumnOrderComparator());
        return res;
    }

    public List<SQLColumn> getGroupColumns() {
        final List<SQLColumn> res = new ArrayList<SQLColumn>();
        for (SQLColumn sqlc : this.columns) {
            if (sqlc.isGroup()) {
                res.add(sqlc);
            }
        }
        Collections.sort(res, new ColumnGroupComparator());
        return res;
    }

    public SQLColumn getWindowColumn() {
        for (SQLColumn sqlc : this.columns) {
            if (sqlc.isWindow()) {
                return sqlc;
            }
        }
        return null;
    }

}
