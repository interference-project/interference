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

import su.interference.core.Config;
import su.interference.sqlexception.InvalidCondition;
import su.interference.sqlexception.InvalidConditionValue;
import su.interference.core.Types;
import su.interference.sqlexception.SQLException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.ArrayList;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class ValueCondition extends Condition { 

    private Object[] values;
    private int id;

    public ValueCondition (SQLColumn cc, int c, String value, NestedCondition nc)
            throws UnsupportedEncodingException, InvalidConditionValue, InvalidCondition {
        super(cc,c,nc);
        if ((((int)value.charAt(0)==34)&&((int)value.charAt(value.length()-1)==34))||(((int)value.charAt(0)==39)&&((int)value.charAt(value.length()-1)==39))) {
            if ((c == Condition.C_EQUAL)||(c == Condition.C_NOT_EQUAL)||(c == Condition.C_LIKE)||(c == Condition.C_NOT_EQUAL)) {
                if (cc.getColumn().getType().getName().equals(Types.t_string)) {
                    if (value.length() >= 2) {
                        if (value.length() == 2) {
                            this.values = new Object[1];
                            this.values[0] = null;
                        } else {
                            this.values = new Object[1];
                            this.values[0] = new String(value.substring(1, value.length() - 1));
                        }
                    } else {
                        throw new InvalidConditionValue();
                    }
                } else if (cc.getColumn().getType().getName().equals(Types.t_date)) {
                    if (value.length()>=2) {
                        if (value.length()==2) {
                            this.values = new Object[1];
                            this.values[0] = null;
                        } else {
                            SimpleDateFormat sdf = new SimpleDateFormat(Config.getConfig().DATEFORMAT);
                            this.values = new Object[1];
                            try {
                                this.values[0] = sdf.parse(new String(value.substring(1, value.length() - 1)));
                            } catch(ParseException e) {
                                throw new InvalidConditionValue("Parse exception date with format "+Config.getConfig().DATEFORMAT);
                            }
                        }
                    } else {
                        throw new InvalidConditionValue();
                    }
                } else {
                    throw new InvalidCondition();
                }
            } else {
                throw new InvalidConditionValue();
            }

        } else if (((int)value.charAt(0)==91)&&((int)value.charAt(value.length()-1)==93)) { //array or SQL statement
            if ((c == Condition.C_IN)||(c == Condition.C_NOT_IN)) {
                if (value.substring(1,value.length()-1).trim().length()>6&&value.substring(1,value.length()-1).trim().toUpperCase().substring(0,6).equals("SELECT")) {
                    SQLSelect sqls = new SQLSelect();
                } else {
                    StringTokenizer st = new StringTokenizer(value.substring(1,value.length()-1).trim(),",");
                    ArrayList<String> cl = new ArrayList<String>();
                    while (st.hasMoreTokens()) {
                        cl.add(st.nextToken().trim());
                    }
                    String[] inarr = cl.toArray(new String[]{});
                    this.values = new Object[inarr.length];
                    for (int i=0; i<inarr.length; i++) {
                        String val = inarr[i].trim();
                        if ((((int)val.charAt(0)==34)&&((int)val.charAt(value.length()-1)==34))||(((int)val.charAt(0)==39)&&((int)val.charAt(value.length()-1)==39))) {
                            if (cc.getColumn().getType().getName().equals(Types.t_string)) {
                                if (value.length()>=2) {
                                    if (value.length()==2) {
                                        this.values[i] = null;
                                    } else {
                                        this.values[i] = new String(val.substring(1,val.length()-1));
                                    }
                                }
                            } else {
                                throw new InvalidConditionValue();
                            }
                        } else { //numeric value
                            if (val.indexOf(".")>=0) { //decimal
                                String ct = cc.getColumn().getType().getName();
                                if (ct.equals(Types.t_float)||ct.equals(Types.p_float)||ct.equals(Types.t_double)||ct.equals(Types.p_double)) {
                                    this.values[i] = Double.parseDouble(val);
                                } else {
                                    throw new InvalidConditionValue();
                                }
                            } else { //integer
                                String ct = cc.getColumn().getType().getName();
                                if (ct.equals(Types.t_int)||ct.equals(Types.p_int)||ct.equals(Types.c_int)) {
                                    this.values[i] = Integer.parseInt(val);
                                } else if (ct.equals(Types.t_long)||ct.equals(Types.p_long)||ct.equals(Types.c_long)) {
                                    this.values[i] = Long.parseLong(val);
                                } else {
                                    throw new InvalidConditionValue();
                                }
                            }
                        }
                    }
                }
            } else {
                throw new InvalidCondition();
            }
        } else { //numeric values
            if ((c == Condition.C_EQUAL)||(c == Condition.C_NOT_EQUAL)||(c == Condition.C_LESS)||(c == Condition.C_LESS_EQUAL)||(c == Condition.C_MORE)||(c == Condition.C_MORE_EQUAL)) {
                if (value.indexOf(".")>=0) { //decimal
                    String ct = cc.getColumn().getType().getName();
                    if (ct.equals(Types.t_float)||ct.equals(Types.p_float)||ct.equals(Types.t_double)||ct.equals(Types.p_double)) {
                        this.values = new Object[1];
                        this.values[0] = Double.parseDouble(value);
                    } else {
                        throw new InvalidConditionValue();
                    }
                } else { //integer
                    String ct = cc.getColumn().getType().getName();
                    if (ct.equals(Types.t_int)||ct.equals(Types.p_int)||ct.equals(Types.c_int)) {
                        this.values = new Object[1];
                        this.values[0] = Integer.parseInt(value);
                    } else if (ct.equals(Types.t_long)||ct.equals(Types.p_long)||ct.equals(Types.c_long)) {
                            this.values = new Object[1];
                            this.values[0] = Long.parseLong(value);
                    } else {
                        throw new InvalidConditionValue();
                    }
                }
            } else {
                throw new InvalidCondition();
            }
        }

    }

    public Object[] getValues() {
        return values;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

}
