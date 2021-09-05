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

import su.interference.core.IndexDescript;
import su.interference.persistent.Table;
import su.interference.exception.InternalException;

import javax.persistence.Id;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLColumn implements Comparable {

    //stub added by interference refactoring
    private int id;
    private int objectId;

    private final Table table;
    private final Class cl;
    private final Class keycl;
    private final Field column;
    private final Method getter;
    private final Method keyGetter;
    private final Method aliasGetter;
    private final String alias; //alias must be equals with tempColumn.getName()
    private Method setter;
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
    private final static ConcurrentHashMap<String, Class> cache = new ConcurrentHashMap<String, Class>();

    // group function
    public static final int F_COUNT = SQLGroupFunction.F_COUNT;
    public static final int F_SUM = SQLGroupFunction.F_SUM;
    public static final int F_MIN = SQLGroupFunction.F_MIN;
    public static final int F_MAX = SQLGroupFunction.F_MAX;
    public static final int F_AVG = SQLGroupFunction.F_AVG;
    public static final int F_LAST = SQLGroupFunction.F_LAST;

    // other function
    public static final int F_GENERIC = 1;
    public static final int F_TO_NUMBER = 2;
    public static final int F_TO_CHAR = 3;
    public static final int F_TO_DATE = 4;

    static {
        cache.put("int", int.class);
        cache.put("long", long.class);
        cache.put("float", float.class);
        cache.put("double", double.class);
    }

    @SuppressWarnings("unchecked")
    public SQLColumn (Table table, int columnid, Field c, String alias, int ftype, int loc, int orderOrd, int groupOrd, int windowInterval, boolean cursor, boolean useUC) throws Exception {
        this.table = table;
        this.cl = table.getTableClass();
        //todo MJ keys may be more than one
        this.keycl = table.getFirstIndexByColumnName(c.getName()) == null ? null : table.getFirstIndexByColumnName(c.getName()).getTableClass();
        this.getter = this.cl.getMethod("get" + c.getName().substring(0, 1).toUpperCase() + c.getName().substring(1, c.getName().length()), null);
        this.aliasGetter = cursor ? this.cl.getMethod("get" + alias.substring(0, 1).toUpperCase() + alias.substring(1, alias.length()), null) : null;
        this.keyGetter = this.keycl == null ? null : this.keycl.getMethod("get" + c.getName().substring(0, 1).toUpperCase() + c.getName().substring(1, c.getName().length()), null);
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
    public Table getIndex() throws InternalException {
        return table.getFirstIndexByColumnName(column.getName());
    }

    public boolean isUnique() throws InternalException {
        if (useUC) {
            Id a1 = (Id) column.getAnnotation(Id.class);
            IndexDescript ids = table.getIndexDescriptByColumnName(column.getName());
            final boolean uix = ids == null ? false : ids.isUnique();
            return a1 != null || uix;
        }
        return false;
    }

    public boolean isIndexOrUnique() throws InternalException {
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
        if (c.toUpperCase().indexOf("LAST(")==0) {
            return F_LAST;
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

    private Class getClassByName(String name) throws ClassNotFoundException {
        final Class c = cache.get(name);
        if (c != null) {
            return c;
        }
        final Class lc = Class.forName(name);
        cache.put(name, lc);
        return lc;
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

    public Method getGetter() {
        return getter;
    }

    public Method getKeyGetter() {
        return keyGetter;
    }

    public Method getAliasGetter() {
        return aliasGetter;
    }

    @SuppressWarnings("unchecked")
    protected Method getSetter(Class r) throws ClassNotFoundException, NoSuchMethodException {
        if (setter == null) {
            setter = r.getMethod("set"+this.alias.substring(0,1).toUpperCase()+this.alias.substring(1,this.alias.length()), new Class<?>[]{getClassByName(getResultSetType())});
        }
        return setter;
    }
}
