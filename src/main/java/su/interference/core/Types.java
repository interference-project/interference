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

package su.interference.core;

import java.lang.reflect.Field;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class Types {

    public static final String p_byte   = "byte";
    public static final String p_char   = "char";
    public static final String p_int    = "int";
    public static final String p_long   = "long";
    public static final String p_float  = "float";
    public static final String p_double = "double";
    public static final String t_byte   = "java.lang.Byte";
    public static final String t_int    = "java.lang.Integer";
    public static final String t_long   = "java.lang.Long";
    public static final String t_float  = "java.lang.Float";
    public static final String t_double = "java.lang.Double";
    public static final String t_date   = "java.util.Date";
    public static final String t_string = "java.lang.String";
    public static final String c_int    = "java.util.concurrent.atomic.AtomicInteger";
    public static final String c_long   = "java.util.concurrent.atomic.AtomicLong";

    public static boolean sqlCheck (String t1, String t2) {
        if (t1.equals(t_string)&&t2.equals(t_string)) {
            return true;
        }
        if (t1.equals(t_date)&&t2.equals(t_date)) {
            return true;
        }
        if ((t1.equals(p_int)||t1.equals(t_int)||t1.equals(c_int)||t1.equals(p_long)||t1.equals(t_long)||t1.equals(c_long))
          &&(t2.equals(p_int)||t2.equals(t_int)||t2.equals(c_int)||t2.equals(p_long)||t2.equals(t_long)||t2.equals(c_long))) {
            return true;
        }
        return (t1.equals(p_float) || t1.equals(t_float) || t1.equals(p_double) || t1.equals(t_double))
                && (t2.equals(p_float) || t2.equals(t_float) || t2.equals(p_double) || t2.equals(t_double));
    }

    public static int getTypeLength(String type) {
        switch (type) {
            case p_byte:
                return 1;
            case p_char:
            case t_byte:
                return 2;
            case p_int:
            case p_float:
                return 4;
            case p_long:
            case p_double:
                return 8;
            case t_int:
            case c_int:
            case t_float:
                return 5;
            case t_long:
            case c_long:
            case t_double:
            case t_date:
              return 9;
            default:
              return 0;
        }
    }

    public static int getLength (Field f) {
        String type = f.getType().getName();
        return getTypeLength(type);
    }

    public static boolean isVarType (String type) {
        return getTypeLength(type) == 0;
    }

    public static boolean isVarType (Field f) {
        return isVarType(f.getType().getName());
    }

    public static boolean isObjType (Field f) {
        return f.getType().getName().equals("java.lang.Object") || f.getType().getName().equals("su.interference.transport.TransportMessage") ||
                f.getType().getName().equals("su.interference.transport.TransportEvent") || f.getType().getName().equals("su.interference.transport.TransportCallback") ||
                f.getType().getName().equals("su.interference.transport.TransportCallback") || f.getType().getName().equals("su.interference.transport.EventResult") ||
                f.getType().getName().equals("su.interference.sql.FrameApiJoin");
    }

    public static boolean isPrimitiveType (String type) {
        switch (type) {
            case p_byte:
            case p_char:
            case p_int:
            case p_long:
            case p_float:
            case p_double:
                return true;
            default:
                return false;
        }
    }

    public static boolean isPrimitiveType (Field f) {
        return isPrimitiveType(f.getType().getName());
    }
}
