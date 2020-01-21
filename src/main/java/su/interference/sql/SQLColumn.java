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

import su.interference.core.IndexDescript;
import su.interference.persistent.Table;
import su.interference.sqlexception.InvalidColumnDescription;
import su.interference.sqlexception.AmbiguousColumnName;
import su.interference.exception.InternalException;

import javax.persistence.Id;
import javax.persistence.Index;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.lang.reflect.Field;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLColumn implements Comparable {

    //stub added by interference refactoring
    private int id;
    private int objectId;

    private final Table table;
    private final Field column;
    private final String alias; //alias must be equals with tempColumn.getName()
    private boolean result;
    private boolean where;
    private boolean order;
    private boolean group;
    private boolean window;
    private int orderOrd;
    private int groupOrd;
    private int windowInterval;
    //private Index   index;
    private final int ftype;
    private final int loc;
    private final boolean cursor;
    private final boolean useUC;
    private boolean mergeIX;

    // group function
    public static final int F_COUNT = 100;
    public static final int F_SUM = 101;
    public static final int F_MIN = 102;
    public static final int F_MAX = 103;
    public static final int F_AVG = 104;

    // other function
    public static final int F_GENERIC = 1;
    public static final int F_TO_NUMBER = 2;
    public static final int F_TO_CHAR = 3;
    public static final int F_TO_DATE = 4;

    public SQLColumn (Table table, int columnid, Field c, String alias, int ftype, int loc, int orderOrd, int groupOrd, int windowInterval, boolean cursor, boolean useUC) {
        this.table = table;
        this.objectId = table.getObjectId();
        this.id = columnid;
        this.column = c;
        this.alias = alias;
        this.ftype = ftype;
        this.loc = loc;
        if (loc==CList.LOC_RESULT) { this.result = true; }
        if (loc==CList.LOC_WHERE) { this.where = true; }
        if (loc==CList.LOC_ORDER) { this.order = true; }
        if (loc==CList.LOC_GROUP) { this.group = true; }
        if (loc==CList.LOC_WINDOW)  { this.window = true; }
        this.windowInterval = windowInterval;
        this.cursor = cursor;
        this.useUC = useUC;
    }

    //returns index table if exists and current column name use as primary column in index (use for join)
    public Table getIndex() throws InternalException, MalformedURLException, ClassNotFoundException {
        return table.getFirstIndexByColumnName(column.getName());
    }

    public boolean isUnique() throws MalformedURLException, ClassNotFoundException, InternalException {
        if (useUC) {
            Id a1 = (Id) column.getAnnotation(Id.class);
            IndexDescript ids = table.getIndexDescriptByColumnName(column.getName());
            final boolean uix = ids == null ? false : ids.isUnique();
            if (a1 != null || uix) {
                return true;
            }
        }
        return false;
    }

    public boolean isIndexOrUnique() throws MalformedURLException, ClassNotFoundException, InternalException {
        return (getIndex()!=null)||isUnique();
    }

    public String getResultSetType() {
        if (ftype == F_COUNT) {
            return "int";
        }
        if (ftype == F_SUM || ftype == F_AVG) {
            return "long";
        }
        return column.getType().getName();
    }

    public int compareTo(Object obj) {
        final SQLColumn c = (SQLColumn)obj;
        if(this.getId() < c.getId()) {
            return -1;
        } else if(this.getId() > c.getId()) {
            return 1;
        }
        return 0;
    }

    public static int checkFunction (String c) {
        if (c.toUpperCase().indexOf("COUNT(")==0) {
            return F_COUNT;
        }
        if (c.toUpperCase().indexOf("SUM(")==0) {
            return F_SUM;
        }
        if (c.toUpperCase().indexOf("MIN(")==0) {
            return F_MIN;
        }
        if (c.toUpperCase().indexOf("MAX(")==0) {
            return F_MAX;
        }
        if (c.toUpperCase().indexOf("AVG(")==0) {
            return F_AVG;
        }

        if (c.toUpperCase().indexOf("TO_NUMBER(")==0) {
            return F_TO_NUMBER;
        }
        if (c.toUpperCase().indexOf("TO_CHAR(")==0) {
            return F_TO_CHAR;
        }
        if (c.toUpperCase().indexOf("TO_DATE(")==0) {
            return F_TO_DATE;
        }

        return 0;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getObjectId() {
        return objectId;
    }

    public void setObjectId(int objectId) {
        this.objectId = objectId;
    }

    public Field getColumn() {
        return column;
    }

    public Table getTable() {
        return table;
    }

    public boolean isResult() {
        return result;
    }

    public void setResult(boolean result) {
        this.result = result;
    }

    public boolean isWhere() {
        return where;
    }

    public void setWhere(boolean where) {
        this.where = where;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean order) {
        this.order = order;
    }

    public boolean isGroup() {
        return group;
    }

    public void setGroup(boolean group) {
        this.group = group;
    }

    public boolean isWindow() {
        return window;
    }

    public void setWindow(boolean window) {
        this.window = window;
    }

    public int getWindowInterval() {
        return windowInterval;
    }

    public void setWindowInterval(int windowInterval) {
        this.windowInterval = windowInterval;
    }

    public int getOrderOrd() {
        return orderOrd;
    }

    public void setOrderOrd(int orderOrd) {
        this.orderOrd = orderOrd;
    }

    public int getGroupOrd() {
        return groupOrd;
    }

    public void setGroupOrd(int groupOrd) {
        this.groupOrd = groupOrd;
    }

    public String getAlias() {
        return alias;
    }

    public int getFtype() {
        return ftype;
    }

    public int getLoc() {
        return loc;
    }

    public boolean isCursor() {
        return cursor;
    }

    public boolean isMergeIX() {
        return mergeIX;
    }

    public void setMergeIX(boolean mergeIX) {
        this.mergeIX = mergeIX;
    }
}
