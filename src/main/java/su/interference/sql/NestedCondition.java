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

import su.interference.persistent.Session;
import su.interference.persistent.Table;
import su.interference.sqlexception.*;
import su.interference.core.Types;
import su.interference.exception.InternalException;

import java.io.IOException;
import java.util.*;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class NestedCondition extends Condition {

    public static final int C_SINGLE  = 0;

    public static final int C_USE_AND = 1;
    public static final int C_USE_OR  = 2;
    public static final int C_USE_XOR = 3;

    private final ArrayList<Condition> conditions;  // list if conditions (VC,JC,NC)
    private int type;        // 1 - AND, 2 - OR
    private boolean empty;

    public boolean checkNC (Object o, int sqlcid, boolean last) throws UnsupportedEncodingException, InternalException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        boolean res = false;
        if (empty) {
            return true;
        }
        if (this.getType() <= NestedCondition.C_USE_AND) {
            res = true;
        }
        for (Condition c : this.conditions) {
            boolean cres = false;
            if (c.getClass().getName().equals("su.interference.sql.NestedCondition")) {
                final NestedCondition nc = (NestedCondition)c;
                cres = nc.checkNC(o, sqlcid, last);
            }
            if (c.getClass().getName().equals("su.interference.sql.JoinCondition")) {
                JoinCondition jc = (JoinCondition)c;
                if ((jc.getId()==sqlcid)||(jc.getId()==0&&last)) {
                    cres = sqlEquals(o, jc);
                } else {
                    cres = true;
                }
            }
            if (c.getClass().getName().equals("su.interference.sql.ValueCondition")) {
                final ValueCondition vc = (ValueCondition)c;
                if ((vc.getId()==sqlcid)||(vc.getId()==0&&last)) {
                    cres = sqlEquals(o, vc);
                } else {
                    cres = true;
                }
            }
            if (this.getType() <= NestedCondition.C_USE_AND) {
                if (!cres) { return false; } else { res = res&cres; }
            }
            if (this.getType() == NestedCondition.C_USE_OR) {
                if (cres) { return true; } else { res = res|cres; }
            }
        }
        return res;
    }

    public boolean sqlEquals (Object o, ValueCondition vc) throws UnsupportedEncodingException, InternalException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        final Class c = o.getClass();
        boolean res = false;

        for (int j=0; j<c.getDeclaredFields().length; j++) {
            final Field f = c.getDeclaredFields()[j];
            if (f.getName().equals(vc.getConditionColumn().getAlias())) {
                res = sqlEquals(o, f, null, vc.getValues(), vc.getCondition());
            }
        }

        return res;
    }

    public boolean sqlEquals (Object o, JoinCondition jc) throws InternalException, UnsupportedEncodingException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        final Class c = o.getClass();
        boolean res = false;

        Field lf = null;
        Field rf = null;
        for (int j=0; j<c.getDeclaredFields().length; j++) {
            Field f = c.getDeclaredFields()[j];
            if (f.getName().equals(jc.getConditionColumn().getAlias())) {
                lf = f;
            }
            if (f.getName().equals(jc.getConditionColumnRight().getAlias())) {
                rf = f;
            }
        }
        if (lf!=null&&rf!=null) {
            res = sqlEquals(o, lf, rf, null, jc.getCondition());
        }

        return res;

    }

    //rf used for join condition, ro - for value condition
    public boolean sqlEquals (Object o, Field lf, Field rf, Object[] ro, int ctype) throws InternalException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        final Class c = o.getClass();
        final String t1 = lf.getType().getName();
        final String t2 = rf==null?t1:rf.getType().getName();

        if (!Types.sqlCheck(t1, t2)) {
            throw new InternalException();
        }

        if (t1.equals(Types.t_string)&&t2.equals(Types.t_string)) {
            final String d1 = (String)c.getMethod("get"+lf.getName().substring(0,1).toUpperCase()+lf.getName().substring(1,lf.getName().length()), null).invoke(o, null);
            final String[] d2 = new String[rf==null?ro.length:1];
            if (rf==null) {
                for (int i=0;i<ro.length;i++) {
                    d2[i] = (String)ro[i];
                }
            } else {
                d2[0] = (String)c.getMethod("get"+rf.getName().substring(0,1).toUpperCase()+rf.getName().substring(1,rf.getName().length()), null).invoke(o,null);
            }
            if ((ctype==Condition.C_EQUAL)||(ctype==Condition.C_IN)) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.equals(d2[i])) {
                        return true;
                    }
                }
                return false;
            }
            if ((ctype==Condition.C_NOT_EQUAL)||(ctype==Condition.C_NOT_IN)) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.equals(d2[i])) {
                        return false;
                    }
                }
                return true;
            }
            if (ctype==Condition.C_LIKE) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.indexOf(d2[i])>=0) {
                        return true;
                    }
                }
                return false;
            }
            if (ctype==Condition.C_NOT_LIKE) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.indexOf(d2[i])>=0) {
                        return false;
                    }
                }
                return true;
            }
        }
        if (t1.equals(Types.t_date)&&t2.equals(Types.t_date)) {
            final Date d1 = (Date)c.getMethod("get"+lf.getName().substring(0,1).toUpperCase()+lf.getName().substring(1,lf.getName().length()), null).invoke(o, null);
            final Date[] d2 = new Date[rf==null?ro.length:1];
            if (rf==null) {
                for (int i=0;i<ro.length;i++) {
                    d2[i] = (Date)ro[i];
                }
            } else {
                d2[0] = (Date)c.getMethod("get"+rf.getName().substring(0,1).toUpperCase()+rf.getName().substring(1,rf.getName().length()), null).invoke(o,null);
            }
            if ((ctype==Condition.C_EQUAL)||(ctype==Condition.C_IN)) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.equals(d2[i])) {
                        return true;
                    }
                }
                return false;
            }
            if ((ctype==Condition.C_NOT_EQUAL)||(ctype==Condition.C_NOT_IN)) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.equals(d2[i])) {
                        return false;
                    }
                }
                return true;
            }
        }
        if ((t1.equals(Types.p_int)||t1.equals(Types.t_int)||t1.equals(Types.c_int)||t1.equals(Types.p_long)||t1.equals(Types.t_long)||t1.equals(Types.c_long))
          &&(t2.equals(Types.p_int)||t2.equals(Types.t_int)||t2.equals(Types.c_int)||t2.equals(Types.p_long)||t2.equals(Types.t_long)||t2.equals(Types.c_long))) {
            Long d1 = new Long(0);
            Long[] d2 = new Long[rf==null?ro.length:1];
            if ((t1.equals(Types.p_int)||t1.equals(Types.t_int)||t1.equals(Types.c_int))&&(t2.equals(Types.p_int)||t2.equals(Types.t_int)||t2.equals(Types.c_int))) {
                Integer dd = (Integer)c.getMethod("get"+lf.getName().substring(0,1).toUpperCase()+lf.getName().substring(1,lf.getName().length()), null).invoke(o,null);
                d1 = new Long(dd);
                if (rf==null) {
                    for (int i=0;i<ro.length;i++) {
                        d2[i] = new Long((Integer)ro[i]);
                    }
                } else {
                    Integer dr = (Integer)c.getMethod("get"+rf.getName().substring(0,1).toUpperCase()+rf.getName().substring(1,rf.getName().length()), null).invoke(o,null);
                    d2[0] = new Long(dr);
                }
            }
            if ((t1.equals(Types.p_long)||t1.equals(Types.t_long)||t1.equals(Types.c_long))&&(t2.equals(Types.p_long)||t2.equals(Types.t_long)||t2.equals(Types.c_long))) {
                d1 = (Long)c.getMethod("get"+lf.getName().substring(0,1).toUpperCase()+lf.getName().substring(1,lf.getName().length()), null).invoke(o, null);
                d2 = rf==null?(Long[])ro:new Long[]{(Long)c.getMethod("get"+rf.getName().substring(0,1).toUpperCase()+rf.getName().substring(1,rf.getName().length()), null).invoke(o,null)};
            }
            if ((ctype==Condition.C_EQUAL)||(ctype==Condition.C_IN)) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.equals(d2[i])) {
                        return true;
                    }
                }
                return false;
            }
            if ((ctype==Condition.C_NOT_EQUAL)||(ctype==Condition.C_NOT_IN)) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.equals(d2[i])) {
                        return false;
                    }
                }
                return true;
            }
            if (ctype==Condition.C_MORE) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.longValue()>d2[i].longValue()) {
                        return true;
                    }
                }
                return false;
            }
            if (ctype==Condition.C_LESS) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.longValue()<d2[i].longValue()) {
                        return true;
                    }
                }
                return false;
            }
            if (ctype==Condition.C_MORE_EQUAL) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.longValue()>=d2[i].longValue()) {
                        return true;
                    }
                }
                return false;
            }
            if (ctype==Condition.C_LESS_EQUAL) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.longValue()<=d2[i].longValue()) {
                        return true;
                    }
                }
                return false;
            }
        }
        if ((t1.equals(Types.p_float)||t1.equals(Types.t_float)||t1.equals(Types.p_double)||t1.equals(Types.t_double))
          &&(t2.equals(Types.p_float)||t2.equals(Types.t_float)||t2.equals(Types.p_double)||t2.equals(Types.t_double))) {
            Double d1 = new Double(0);
            Double[] d2 = new Double[rf==null?ro.length:1];
            if ((t1.equals(Types.p_float)||t1.equals(Types.t_float))&&(t2.equals(Types.p_float)||t2.equals(Types.t_float))) {
                Float dd = (Float)c.getMethod("get"+lf.getName().substring(0,1).toUpperCase()+lf.getName().substring(1,lf.getName().length()), null).invoke(o,null);
                d1 = new Double(dd);
                if (rf==null) {
                    for (int i=0;i<ro.length;i++) {
                        //RO object created by ValueCondition constructor, and already cast to 8-byte type (Double)
                        d2[i] = (Double)ro[i];
                    }
                } else {
                    Float dr = (Float)c.getMethod("get"+rf.getName().substring(0,1).toUpperCase()+rf.getName().substring(1,rf.getName().length()), null).invoke(o,null);
                    d2[0] = new Double(dr);
                }
            }
            if ((t1.equals(Types.p_double)||t1.equals(Types.t_double))&&(t2.equals(Types.p_double)||t2.equals(Types.t_double))) {
                d1 = (Double)c.getMethod("get"+lf.getName().substring(0,1).toUpperCase()+lf.getName().substring(1,lf.getName().length()), null).invoke(o, null);
                d2 = rf==null?(Double[])ro:new Double[]{(Double)c.getMethod("get"+rf.getName().substring(0,1).toUpperCase()+rf.getName().substring(1,rf.getName().length()), null).invoke(o,null)};
            }
            if ((ctype==Condition.C_EQUAL)||(ctype==Condition.C_IN)) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.equals(d2[i])) {
                        return true;
                    }
                }
                return false;
            }
            if ((ctype==Condition.C_NOT_EQUAL)||(ctype==Condition.C_NOT_IN)) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.equals(d2[i])) {
                        return false;
                    }
                }
                return true;
            }
            if (ctype==Condition.C_MORE) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.doubleValue()>d2[i].doubleValue()) {
                        return true;
                    }
                }
                return false;
            }
            if (ctype==Condition.C_LESS) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.doubleValue()<d2[i].doubleValue()) {
                        return true;
                    }
                }
                return false;
            }
            if (ctype==Condition.C_MORE_EQUAL) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.doubleValue()>=d2[i].doubleValue()) {
                        return true;
                    }
                }
                return false;
            }
            if (ctype==Condition.C_LESS_EQUAL) {
                for (int i=0; i<d2.length; i++) {
                    if (d1.doubleValue()<=d2[i].doubleValue()) {
                        return true;
                    }
                }
                return false;
            }
        }

        return false;
    }

    public NestedCondition (String cdd, SQLStatement sql, ArrayList<SQLTable> tables) throws Exception {
        int nl = 0;
        String cc = "";
        String ic = "";

        //get parsed conditions
        conditions = new ArrayList<Condition>();

        if (!cdd.trim().equals("")) {

            for (int i=0; i<cdd.length(); i++) {
                if (cdd.substring(i,i+1).equals("(")) {
                    nl++;
                } else if (cdd.substring(i,i+1).equals(")")) {
                    nl--;
                } else {
                    if (nl==0) {
                        cc = cc+cdd.substring(i,i+1);
                    } else { //inline condition
                        ic = ic+cdd.substring(i,i+1);
                    }
                }
            }
            if (nl>0) {
                throw new MissingRightParenthesis();
            }
            if (nl<0) {
                throw new UnexpectedEndOfStatement();
            }
            if (!(cc.trim().equals(""))) {
                if ((cc.indexOf(" AND ")>=0||cc.indexOf(" and ")>=0)&&(cc.indexOf(" OR ")>=0||cc.indexOf(" or ")>=0)) {
                    throw new InvalidCondition();
                } else {
                    if (cc.indexOf(" AND ")>=0) {
                        this.type = C_USE_AND;
                        String[] ccs = cc.split(" AND ");
                        conditions.addAll(getParsedConditions(ccs,sql,tables));
                    } else if (cc.indexOf(" and ")>=0) {
                            this.type = C_USE_AND;
                            String[] ccs = cc.split(" and ");
                            conditions.addAll(getParsedConditions(ccs,sql,tables));
                    } else if (cc.indexOf(" OR ")>=0) {
                        this.type = C_USE_OR;
                        String[] ccs = cc.split(" OR ");
                        conditions.addAll(getParsedConditions(ccs,sql,tables));
                    } else if (cc.indexOf(" or ")>=0) {
                        this.type = C_USE_OR;
                        String[] ccs = cc.split(" or ");
                        conditions.addAll(getParsedConditions(ccs,sql,tables));
                    } else { //simply one condition expected
                        this.type = C_SINGLE;
                        String[] ccs = new String[]{cc.trim()};
                        conditions.addAll(getParsedConditions(ccs,sql,tables));
                    }
                }
            } else {
                throw new InvalidCondition();
            }
            //parse nested conditions, join conditions to current level, if types is equals
            //hack - inc created only for check
            if (!(ic.trim().equals(""))) {
                NestedCondition inc = new NestedCondition(ic,sql,tables);
                if (inc.getType()==C_SINGLE||inc.getType()==this.type) {
                    conditions.addAll(inc.getConditions());
                } else {
                    conditions.add(new NestedCondition(ic,sql,tables));
                }
            }

        } else {
            empty = true;
        }
    }

    private ArrayList<Condition> getParsedConditions (String[] cdds, SQLStatement sql, ArrayList<SQLTable> tables) throws Exception {
        ArrayList<Condition> res = new ArrayList<Condition>();
        if (cdds.length>0) {
            for (int i=0; i<cdds.length; i++) {
                if (!(cdds[i].trim().equals(""))) {
                    int n = 0;
                    int c = 0;
                    int x = 0;
                    if (cdds[i].indexOf("=")>=0) {
                        n = cdds[i].indexOf("=");
                        c = Condition.C_EQUAL;
                        x = 1;
                    }
                    if (cdds[i].indexOf("<")>=0) {
                        n = cdds[i].indexOf("<");
                        c = Condition.C_LESS;
                        x = 1;
                    }
                    if (cdds[i].indexOf(">")>=0) {
                        n = cdds[i].indexOf(">");
                        c = Condition.C_MORE;
                        x = 1;
                    }
                    if (cdds[i].indexOf("<=")>=0) {
                        n = cdds[i].indexOf("<=");
                        c = Condition.C_LESS_EQUAL;
                        x = 2;
                    }
                    if (cdds[i].indexOf(">=")>=0) {
                        n = cdds[i].indexOf(">=");
                        c = Condition.C_MORE_EQUAL;
                        x = 2;
                    }
                    if (cdds[i].indexOf("<>")>=0) {
                        n = cdds[i].indexOf("<>");
                        c = Condition.C_NOT_EQUAL;
                        x = 2;
                    }
                    if (cdds[i].indexOf("IN")>=0) {
                        n = cdds[i].indexOf("IN");
                        c = Condition.C_IN;
                        x = 2;
                    }
                    if (cdds[i].indexOf("in")>=0) {
                        n = cdds[i].indexOf("in");
                        c = Condition.C_IN;
                        x = 2;
                    }
                    if (cdds[i].indexOf("NOT IN")>=0) {
                        n = cdds[i].indexOf("NOT IN");
                        c = Condition.C_NOT_IN;
                        x = 6;
                    }
                    if (cdds[i].indexOf("not in")>=0) {
                        n = cdds[i].indexOf("not in");
                        c = Condition.C_NOT_IN;
                        x = 6;
                    }
                    if (cdds[i].indexOf("like")>=0) {
                        n = cdds[i].indexOf("like");
                        c = Condition.C_LIKE;
                        x = 4;
                    }
                    if (cdds[i].indexOf("NOT LIKE")>=0) {
                        n = cdds[i].indexOf("NOT LIKE");
                        c = Condition.C_NOT_LIKE;
                        x = 8;
                    }
                    if (cdds[i].indexOf("not like")>=0) {
                        n = cdds[i].indexOf("not like");
                        c = Condition.C_NOT_LIKE;
                        x = 8;
                    }
                    if (c>0) {
                        final String c_left  = cdds[i].substring(0,n);
                        final String c_right = cdds[i].substring(n+x,cdds[i].length());
                        SQLColumn sqlc_l = null;
                        SQLColumn sqlc_r = null;
                        if (sql!=null) {
                            sqlc_l = sql.getCols().check(c_left.trim(),c_left.trim(),CList.LOC_WHERE,0);
                            sqlc_r = sql.getCols().check(c_right.trim(),c_right.trim(),CList.LOC_WHERE,0);
                        } else {
                            //todo ? some exception
                        }

                        if ((sqlc_l==null)&&(sqlc_r==null)) {
                            throw new InvalidCondition();
                        }

                        if ((!(sqlc_l==null))&&(sqlc_r==null)) {
                            res.add(new ValueCondition(sqlc_l, c, c_right.trim(), this));
                        }
                        if ((sqlc_l==null)&&(!(sqlc_r==null))) {
                            res.add(new ValueCondition(sqlc_r, c, c_left.trim(), this));
                        }
                        if ((!(sqlc_l==null))&&(!(sqlc_r==null))) {
                            res.add(new JoinCondition(sqlc_l, c, sqlc_r, this));
                        }
                    } else {
                        throw new InvalidCondition();
                    }
                }
            }
        } else { // l = 0
            throw new InvalidCondition();
        }
        return res;
    }


    public ArrayList<ValueCondition> getValueConditions() {
        final ArrayList<ValueCondition> res = new ArrayList<ValueCondition>();
        ArrayList<Condition> cs = this.conditions;
        while (cs!=null) {
            boolean isnc = false;
            for (Condition c : cs) {
                if (c.getClass().getName().equals("su.interference.sql.NestedCondition")) {
                    final NestedCondition nc = (NestedCondition)c;
                    cs = nc.getConditions();
                    isnc = true;
                } else if (c.getClass().getName().equals("su.interference.sql.ValueCondition")) {
                    final ValueCondition vc = (ValueCondition)c;
                    res.add(vc);
                }
            }
            if (!isnc) {
                cs = null;
            }
        }
        return res;
    }

    public ArrayList<Condition> getAllConditions() {
        final ArrayList<Condition> res = new ArrayList<Condition>();
        ArrayList<Condition> cs = this.conditions;
        while (cs!=null) {
            boolean isnc = false;
            for (Condition c : cs) {
                if (c.getClass().getName().equals("su.interference.sql.NestedCondition")) {
                    final NestedCondition nc = (NestedCondition)c;
                    cs = nc.getConditions();
                    isnc = true;
                    res.add(nc);
                } else if (c.getClass().getName().equals("su.interference.sql.ValueCondition")) {
                    final ValueCondition vc = (ValueCondition)c;
                    res.add(vc);
                } else if (c.getClass().getName().equals("su.interference.sql.JoinCondition")) {
                    final JoinCondition jc = (JoinCondition)c;
                    res.add(jc);
                }
            }
            if (!isnc) {
                cs = null;
            }
        }
        return res;
    }

    public SQLJoinDispatcher getJoinDispatcher(FrameIterator lbi, FrameIterator rbi, ArrayList<SQLColumn> rscols, Session s) throws IOException, ClassNotFoundException, InternalException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        ArrayList<Condition> cs = this.conditions;
        int ctype = this.type;
        //in cursor we can not guaranteed uniqueness of base unique field
        if (lbi==null||rbi==null) return null;
        //if (lbi.getType()==FrameIterator.TYPE_CURSOR||rbi.getType()==FrameIterator.TYPE_CURSOR) return null;
        if (ctype==C_USE_OR) return null;
        List<SQLJoinDispatcher> jlist = new ArrayList<>();
        while (cs!=null) {
            boolean isnc = false;
            for (Condition c : cs) {
                if (c.getClass().getName().equals("su.interference.sql.NestedCondition")) {
                    final NestedCondition nc = (NestedCondition)c;
                    cs = nc.getConditions();
                    ctype = nc.getType();
                    if (ctype==C_USE_OR) return null;
                    isnc = true;
                } else if (c.getClass().getName().equals("su.interference.sql.JoinCondition")) {
                    final JoinCondition jc = (JoinCondition)c;
                    final SQLColumn cc = getSQLColumnByAlias(rscols, jc.getConditionColumn().getAlias());
                    final SQLColumn ccr = getSQLColumnByAlias(rscols, jc.getConditionColumnRight().getAlias());;
                    //if (jc.getConditionColumn().isIndexOrUnique()||jc.getConditionColumnRight().isIndexOrUnique()) {
                        if (lbi.getObjectIds().contains(jc.getConditionColumn().getObjectId()) && rbi.getObjectIds().contains(jc.getConditionColumnRight().getObjectId())) {
                            jlist.add(new SQLJoinDispatcher(lbi, rbi, cc, ccr, getSkipCheck(jc), this, s));
                        } else if (lbi.getObjectIds().contains(jc.getConditionColumnRight().getObjectId()) && rbi.getObjectIds().contains(jc.getConditionColumn().getObjectId())) {
                            jlist.add(new SQLJoinDispatcher(rbi, lbi, cc, ccr, getSkipCheck(jc), this, s));
                        }
                    //}
                }
            }
            if (!isnc) {
                cs = null;
            }
        }
        Collections.sort(jlist);
        return jlist.size()==0?null:jlist.get(0);
    }

    //todo returns first found index vc for table if ix == null
    public ValueCondition getIndexVC(FrameIterator bi, Table ix) throws IOException, ClassNotFoundException, InternalException {
        ArrayList<Condition> cs = this.conditions;
        int ctype = this.type;
        if (bi==null) return null;
        if (ctype==C_USE_OR) return null;
        if (bi.getType()== FrameIterator.TYPE_CURSOR) return null;
        while (cs!=null) {
            boolean isnc = false;
            for (Condition c : cs) {
                if (c.getClass().getName().equals("su.interference.sql.NestedCondition")) {
                    final NestedCondition nc = (NestedCondition)c;
                    cs = nc.getConditions();
                    ctype = nc.getType();
                    if (ctype==C_USE_OR) return null;
                    isnc = true;
                } else if (c.getClass().getName().equals("su.interference.sql.ValueCondition")) {
                    final ValueCondition vc = (ValueCondition)c;
                    if (vc.getConditionColumn().getIndex() != null && (ix == null || vc.getConditionColumn().getIndex().getObjectId() == ix.getObjectId())) {
                        if (bi.getObjectIds().contains(vc.getConditionColumn().getObjectId())) {
                            return vc;
                        }
                    }
                }
            }
            if (!isnc) {
                cs = null;
            }
        }
        return null;
    }

    private SQLColumn getSQLColumnByAlias(ArrayList<SQLColumn> rscols, String alias) {
        for (SQLColumn sqlc : rscols) {
            if (sqlc.getAlias().equals(alias)) {
                return sqlc;
            }
        }
        return null;
    }

    //todo (OR) conditions only?
    private boolean getSkipCheck(JoinCondition jcc) {
        ArrayList<Condition> cs = this.conditions;
        boolean res = false;
        while (cs!=null) {
            boolean isnc = false;
            for (Condition c : cs) {
                if (c.getClass().getName().equals("su.interference.sql.NestedCondition")) {
                    final NestedCondition nc = (NestedCondition)c;
                    cs = nc.getConditions();
                    isnc = true;
                } else if (c.getClass().getName().equals("su.interference.sql.JoinCondition")) {
                    final JoinCondition jc = (JoinCondition)c;
                    if (jc!=jcc) {
                        if ((jc.getConditionColumn().getObjectId()==jcc.getConditionColumn().getObjectId()&&jc.getConditionColumnRight().getObjectId()==jcc.getConditionColumnRight().getObjectId())
                            ||(jc.getConditionColumn().getObjectId()==jcc.getConditionColumnRight().getObjectId()&&jc.getConditionColumnRight().getObjectId()==jcc.getConditionColumn().getObjectId()))
                        {
                            res = true;
                        }
                    }
                } else if (c.getClass().getName().equals("su.interference.sql.ValueCondition")) {
                    final ValueCondition vc = (ValueCondition)c;
                    if (vc.getConditionColumn().getObjectId()==jcc.getConditionColumn().getObjectId()||vc.getConditionColumn().getObjectId()==jcc.getConditionColumnRight().getObjectId()) {
                        res = true;
                    }
                }
            }
            if (!isnc) {
                cs = null;
            }
        }
        return res;
    }

    public void coordinateConditions(SQLTable t1, SQLTable t2, int id) {
        ArrayList<Condition> cs = this.conditions;
        int ctype = this.type;
        while (cs!=null) {
            boolean isnc = false;
            for (Condition c : cs) {
                if (c.getClass().getName().equals("su.interference.sql.NestedCondition")) {
                    final NestedCondition nc = (NestedCondition)c;
                    cs = nc.getConditions();
                    ctype = nc.getType();
                    isnc = true;
                } else if (c.getClass().getName().equals("su.interference.sql.JoinCondition")) {
                    final JoinCondition jc = (JoinCondition)c;
                    if ((jc.getConditionColumn().getObjectId()==t1.getTable().getObjectId()&&jc.getConditionColumnRight().getObjectId()==t2.getTable().getObjectId())
                        ||(jc.getConditionColumn().getObjectId()==t2.getTable().getObjectId()&&jc.getConditionColumnRight().getObjectId()==t1.getTable().getObjectId())) {
                        if (ctype==C_USE_AND) {
                            jc.setId(id);
                        }
                    }
                } else if (c.getClass().getName().equals("su.interference.sql.ValueCondition")) {
                    final ValueCondition vc = (ValueCondition)c;
                    if (vc.getConditionColumn().getObjectId()==t1.getTable().getObjectId()||vc.getConditionColumn().getObjectId()==t2.getTable().getObjectId()) {
                        if (ctype==C_USE_AND) {
                            vc.setId(id);
                        }
                    }
                }
            }
            if (!isnc) {
                cs = null;
            }
        }
    }

    public ArrayList<Condition> getConditions() {
        return conditions;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

}
