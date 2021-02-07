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

package su.interference.serialize;

import su.interference.api.SerializerApi;
import su.interference.core.*;
import su.interference.exception.InternalException;
import su.interference.persistent.Table;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class CustomSerializer implements SerializerApi {

    private byte[] b = new byte[]{};
    private final boolean external;

    public CustomSerializer() {
        this.external = false;
    }

    public CustomSerializer(boolean external) {
        this.external = external;
    }

    public CustomSerializer(String t, Object o) throws IllegalAccessException, UnsupportedEncodingException, ClassNotFoundException, InternalException, InstantiationException {
        b = serialize(t, o);
        this.external = false;
    }

    public CustomSerializer(byte[] b) {
        this.b = b;
        this.external = false;
    }

    public byte[] getBytes() {
        return b;
    }

    public byte[] serialize (String t, Object fo) throws IllegalAccessException, UnsupportedEncodingException, ClassNotFoundException, InternalException, InstantiationException {
        if (!Types.isPrimitiveType(t)) {
            switch (t) {
                case ("java.lang.Integer"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromInt(fo==null?0:(Integer)fo));
                case ("java.lang.Long"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromLong(fo==null?0:(Long)fo));
                case ("java.util.concurrent.atomic.AtomicInteger"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromInt(fo==null?0:((AtomicInteger)fo).get()));
                case ("java.util.concurrent.atomic.AtomicLong"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromLong(fo==null?0:((AtomicLong)fo).get()));
                case ("java.lang.Float"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromFloat(fo==null?0:(Float)fo));
                case ("java.lang.Double"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromDouble(fo==null?0:(Double)fo));
                case ("java.util.Date"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromDate((Date)fo));
                case ("java.lang.String"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromString((String)fo));
                case ("java.util.ArrayList"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromArrayList((ArrayList)fo));
                case ("[B"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},(byte[])fo);
                case ("su.interference.core.DataChunk"):
                    return append(fo==null?new byte[]{0}:new byte[]{1}, append(getBytesFromInt(((DataChunk)fo).getT().getObjectId()),
                            append(getBytesFromInt(((DataChunk)fo).getHeader().getRowID().getFileId()), getBytesFromLong(((DataChunk)fo).getHeader().getRowID().getFramePointer())),
                            append(((DataChunk)fo).getHeader().getHeader(), ((DataChunk)fo).getChunk())));
                case ("su.interference.transport.TransportMessage"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromObject(fo));
                case ("su.interference.transport.TransportEvent"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromObject(fo));
                case ("su.interference.transport.TransportCallback"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromObject(fo));
                case ("su.interference.transport.EventResult"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromObject(fo));
                case ("su.interference.sql.FrameApiJoin"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromObject(fo));
                case ("java.lang.Class"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},fo==null?new byte[]{}:getBytesFromString(((Class) fo).getName()));
                case ("java.lang.Exception"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},fo==null?new byte[]{}:getBytesFromString(((Exception) fo).getClass().getSimpleName()+": "+((Exception) fo).getMessage()));
                case ("java.lang.Object"):
                    return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromObject(fo));
            }
        } else {
            switch (t) {
                case ("byte"):
                    byte[] res = new byte[1];
                    res[0] = (Byte) fo;
                    return res;
                case ("char"):
                    return getBytesFromChar((Character) fo);
                case ("int"):
                    return getBytesFromInt((Integer) fo);
                case ("long"):
                    return getBytesFromLong((Long) fo);
                case ("float"):
                    return getBytesFromFloat((Float) fo);
                case ("double"):
                    return getBytesFromDouble((Double) fo);
            }
        }
        return null;
    }

    @Deprecated
    public int length (String t, Object fo) throws IllegalAccessException, UnsupportedEncodingException, ClassNotFoundException, InternalException, InstantiationException {
        if (!Types.isPrimitiveType(t)) {
            switch (t) {
                case ("java.lang.Integer"):
                case ("java.util.concurrent.atomic.AtomicInteger"):
                case ("java.lang.Float"):
                    return 5;
                case ("java.lang.Long"):
                case ("java.util.concurrent.atomic.AtomicLong"):
                case ("java.lang.Double"):
                case ("java.util.Date"):
                    return 9;
                case ("java.lang.String"):
                    return getBytesFromString((String)fo).length + 1;
                case ("java.util.ArrayList"):
                    return getBytesFromArrayList((ArrayList)fo).length + 1;
                case ("[B"):
                    return ((byte[])fo).length + 1;
                case ("su.interference.core.DataChunk"):
                    return append(getBytesFromInt(((DataChunk)fo).getT().getObjectId()),
                            append(getBytesFromInt(((DataChunk)fo).getHeader().getRowID().getFileId()), getBytesFromLong(((DataChunk)fo).getHeader().getRowID().getFramePointer())),
                            append(((DataChunk)fo).getHeader().getHeader(), ((DataChunk)fo).getChunk())).length + 1;
            }
        } else {
            switch (t) {
                case ("byte"):
                    return 1;
                case ("char"):
                    return getBytesFromChar((Character) fo).length;
                case ("int"):
                case ("float"):
                    return 4;
                case ("long"):
                case ("double"):
                    return 8;
            }
        }
        return 0;
    }

    public Object deserialize (byte[] b, Field f) throws InvocationTargetException, NoSuchMethodException, UnsupportedEncodingException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException {
        final String t = f.getType().getName();
        final String g = f.getGenericType().toString();
        return deserialize(b, t, g, null);
    }

    public Object deserialize (byte[] b, Field f, String cn) throws InvocationTargetException, NoSuchMethodException, UnsupportedEncodingException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException {
        final String t = f == null ? cn : f.getType().getName();
        final String g = f == null ? cn : f.getGenericType().toString();
        return deserialize(b, t, g, cn);
    }

    public Object deserialize (byte[] b, String t, String g, String cn) throws InvocationTargetException, NoSuchMethodException, UnsupportedEncodingException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException {
        if (!Types.isPrimitiveType(t)) {
            int isNull = getIntFromBytes(substring(b,0,1));
            if (isNull==0) {
                return null;
            }
            switch (t) {
                case ("java.lang.Integer"):
                    return new Integer(getIntFromBytes(substring(b, 1, b.length)));
                case ("java.lang.Long"):
                    return new Long(getLongFromBytes(substring(b, 1, b.length)));
                case ("java.util.concurrent.atomic.AtomicInteger"):
                    return new AtomicInteger(getIntFromBytes(substring(b, 1, b.length)));
                case ("java.util.concurrent.atomic.AtomicLong"):
                    return new AtomicLong(getLongFromBytes(substring(b, 1, b.length)));
                case ("java.lang.Float"):
                    return new Float(getFloatFromBytes(substring(b, 1, b.length)));
                case ("java.lang.Double"):
                    return new Double(getDoubleFromBytes(substring(b, 1, b.length)));
                case ("java.lang.Byte"):
                    return new Byte(b[1]);
                case ("java.util.Date"):
                    return getDateFromBytes(substring(b, 1, b.length));
                case ("java.lang.String"):
                    return getStringFromBytes(substring(b, 1, b.length));
                case ("java.util.ArrayList"):
                    return getArrayListFromBytes(substring(b, 1, b.length), g);
                case ("[B"):
                    return substring(b, 1, b.length);
                case ("su.interference.core.DataChunk"):
                    final Table to = Instance.getInstance().getTableById(getIntFromBytes(substring(b, 1, 5)));
                    final int file = getIntFromBytes(substring(b, 5, 9));
                    final long frame = getLongFromBytes(substring(b, 9, 17));
                    final int endpos = to.isIndex() ? 49 : 33;
                    final RowHeader h = new RowHeader(substring(b, 17, endpos), file, frame);
                    return new DataChunk(substring(b, endpos, b.length), to, h, null);
                case ("su.interference.transport.TransportMessage"):
                    return getObjectFromBytes(substring(b, 1, b.length), cn);
                case ("su.interference.transport.TransportEvent"):
                    return getObjectFromBytes(substring(b, 1, b.length), cn);
                case ("su.interference.transport.TransportCallback"):
                    return getObjectFromBytes(substring(b, 1, b.length), cn);
                case ("su.interference.transport.EventResult"):
                    return getObjectFromBytes(substring(b, 1, b.length), cn);
                case ("su.interference.sql.FrameApiJoin"):
                    return getObjectFromBytes(substring(b, 1, b.length), cn);
                case ("java.lang.Class"):
                    return Class.forName(getStringFromBytes(substring(b, 1, b.length)));
                case ("java.lang.Exception"):
                    return new RuntimeException(getStringFromBytes(substring(b, 1, b.length)));
                case ("java.lang.Object"):
                    return getObjectFromBytes(substring(b, 1, b.length), cn);
            }
        } else {
            switch (t) {
                case ("byte"):
                    return b[0];
                case ("int"):
                    return getIntFromBytes(b);
                case ("long"):
                    return getLongFromBytes(b);
                case ("float"):
                    return getFloatFromBytes(b);
                case ("double"):
                    return getDoubleFromBytes(b);
                case ("char"):
                    return getCharFromBytes(b);
            }
        }
        return null;
    }

    // get value from string
    public Object deserialize (String v, String t) throws UnsupportedEncodingException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException, ParseException {
        if (!Types.isPrimitiveType(t)) {
            if (v.length()==0) {
                return null;
            }
            switch (t) {
                case ("java.lang.Integer"):
                    return Integer.parseInt(v);
                case ("java.lang.Long"):
                    return Long.parseLong(v);
                case ("java.util.concurrent.atomic.AtomicInteger"):
                    return new AtomicInteger(Integer.parseInt(v));
                case ("java.util.concurrent.atomic.AtomicLong"):
                    return new AtomicLong(Long.parseLong(v));
                case ("java.lang.Float"):
                    return Float.parseFloat(v);
                case ("java.lang.Double"):
                    return Double.parseDouble(v);
                case ("java.util.Date"):
                    SimpleDateFormat df = new SimpleDateFormat(this.external ? Config.getConfig().DATEFORMAT : Instance.getInstance() == null ? Config.getConfig().DATEFORMAT : Instance.getInstance().getDateFormat());
                    return df.parse(v);
                case ("java.lang.String"):
                    return v;
            }
        } else {
            switch (t) {
                case ("int"):
                    return Integer.parseInt(v);
                case ("long"):
                    return Long.parseLong(v);
                case ("float"):
                    return Float.parseFloat(v);
                case ("double"):
                    return Double.parseDouble(v);
            }
        }
        return null;
    }

    private byte[] getBytesFromInt (int p) {
        final ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(p);
        return bb.array();
    }

    private byte[] getBytesFromLong (long p) {
        final ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(p);
        return bb.array();
    }

    private byte[] getBytesFromFloat (float p) {
        final ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putFloat(p);
        return bb.array();
    }

    private byte[] getBytesFromDouble (double p) {
        final ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putDouble(p);
        return bb.array();
    }

    private byte[] getBytesFromChar (char p) {
        final ByteBuffer bb = ByteBuffer.allocate(2);
        bb.putChar(p);
        return bb.array();
    }

    private byte[] getBytesFromString (String p) throws UnsupportedEncodingException, ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        return p==null?new byte[]{}:p.getBytes(this.external ? Config.getConfig().CODEPAGE : Instance.getInstance() == null ? Config.getConfig().CODEPAGE : Instance.getInstance().getCodePage());
    }

    private byte[] getBytesFromArrayList (ArrayList p) throws UnsupportedEncodingException, IllegalAccessException, ClassNotFoundException, InternalException, InstantiationException {
        byte[] res = new byte[]{};
        String cn;
        if (p!=null) {
            if (p.size()>0) {
                cn = p.get(0).getClass().getName();
                for (Object o : p) {
                    byte[] b = serialize(cn, o);
                    if (Types.isVarType(cn)) {
                        res = append(res, getBytesFromInt(b.length), b);
                    } else {
                        res = append(res, b);
                    }
                }
            }
        }
        return res;
    }

    private byte[] getBytesFromDate (Date p) {
        return p==null?getBytesFromLong(0):getBytesFromLong(p.getTime());
    }

    private int getIntFromBytes(byte[] b) {
        final ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(b);
        bb.rewind();
        return bb.getInt();
    }

    private long getLongFromBytes(byte[] b) {
        final ByteBuffer bb = ByteBuffer.allocate(8);
        bb.put(b);
        bb.rewind();
        return bb.getLong();
    }

    private float getFloatFromBytes(byte[] b) {
        final ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(b);
        bb.rewind();
        return bb.getFloat();
    }

    private double getDoubleFromBytes(byte[] b) {
        final ByteBuffer bb = ByteBuffer.allocate(8);
        bb.put(b);
        bb.rewind();
        return bb.getDouble();
    }

    private char getCharFromBytes(byte[] b) {
        final ByteBuffer bb = ByteBuffer.allocate(2);
        bb.put(b);
        bb.rewind();
        return bb.getChar();
    }

    private String getStringFromBytes (byte[] b) throws UnsupportedEncodingException, ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        return new String(b, this.external ? Config.getConfig().CODEPAGE : Instance.getInstance() == null ? Config.getConfig().CODEPAGE : Instance.getInstance().getCodePage());
    }

    @SuppressWarnings("unchecked")
    private ArrayList getArrayListFromBytes (byte[] b, String t) throws InvocationTargetException, NoSuchMethodException, UnsupportedEncodingException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException {
        final ArrayList r = new ArrayList();
        if (b!=null) {
            if (b.length>0) {
                if (t.indexOf("<")>=0&&t.indexOf(">")>0) {
                    String et = t.substring(t.indexOf("<")+1,t.lastIndexOf(">"));
                    if (et.indexOf("<")>=0) {
                        et = et.substring(0, et.indexOf("<"));
                    }
                    int v = Types.isVarType(et)?4:0;
                    int s = 0;
                    boolean cnue = true;
                    while (cnue) {
                        byte[] data = substring(b, s+v, s+v+(Types.isVarType(et)?getIntFromBytes(substring(b,s,s+v)):Types.getTypeLength(et,0)));
                        s = s + data.length + v;
                        r.add(deserialize(data, et, et, null));
                        if (s>=b.length) { cnue=false; }
                    }
                }
            }
        }
        return r;
    }

    private Date getDateFromBytes (byte[] b) {
        final Date d = new Date();
        d.setTime(getLongFromBytes(b));
        return d;
    }

    private Object getObjectFromBytes(byte[] b, String t) throws InvocationTargetException, NoSuchMethodException, UnsupportedEncodingException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException {
        final Class c = Class.forName(t);
        final Field[] fields = c.getDeclaredFields();
        if (t.equals("[B")) {
            return b;
        }
        if (t.equals("java.lang.Integer")) {
            return getIntFromBytes(b);
        }
        if (t.equals("java.lang.Long")) {
            return getLongFromBytes(b);
        }
        if (t.equals("java.lang.String")) {
            return new String(b,"UTF-8");
        }
        final Object o = c.getConstructor().newInstance();
        final ByteString bs = new ByteString(b);
        int s = 0;
        for (int i = 0; i < fields.length; i++) {
            final boolean obj = Types.isObjType(fields[i]);
            final int v = Types.isVarType(fields[i]) ? 4 : 0;
            final int m = fields[i].getModifiers();
            if (!Modifier.isTransient(m) && !Modifier.isStatic(m)) {
                if (obj) {
                    String cn = null;
                    final int l_ = bs.getIntFromBytes(bs.substring(s, s + 4));
                    if (l_ == 0) {
                        s = s + 4;
                    } else {
                        final byte[] data_ = bs.substring(s + 4, s + 4 + l_);
                        cn = new String(data_, "UTF-8");
                        s = s + data_.length + 4;
                    }
                    final byte[] data = bs.substring(s + 4, s + 4 + bs.getIntFromBytes(bs.substring(s, s + 4)));
                    if (data.length > 0) {
                        if (Modifier.isPrivate(m)) {
                            fields[i].setAccessible(true);
                        }
                        fields[i].set(o, this.deserialize(data, fields[i], cn));
                    }
                    s = s + data.length + 4;
                } else {
                    final byte[] data = bs.substring(s + v, s + v + (Types.isVarType(fields[i]) ? bs.getIntFromBytes(bs.substring(s, s + v)) : Types.getLength(fields[i])));
                    if (data.length > 0) {
                        if (Modifier.isPrivate(m)) {
                            fields[i].setAccessible(true);
                        }
                        fields[i].set(o, this.deserialize(data, fields[i]));
                    }
                    s = s + data.length + v;
                }
            }
        }
        return o;
    }

    private byte[] getBytesFromObject(Object o) throws ClassNotFoundException, InstantiationException, UnsupportedEncodingException, IllegalAccessException, InternalException {
        if (o == null) {
            return new byte[]{};
        }
        final String t = o.getClass().getName();
        if (t.equals("[B")) {
            return (byte[])o;
        }
        if (t.equals("java.lang.Integer")) {
            return getBytesFromInt((int)o);
        }
        if (t.equals("java.lang.Long")) {
            return getBytesFromLong((long)o);
        }
        if (t.equals("java.lang.String")) {
            return ((String) o).getBytes("UTF-8");
        }
        final Field[] fields = o.getClass().getDeclaredFields();
        final ByteString res = new ByteString();
        for (int i = 0; i < fields.length; i++) {
            final int m = fields[i].getModifiers();
            if (!Modifier.isTransient(m) && !Modifier.isStatic(m)) {
                byte[] b;
                if (Modifier.isPrivate(m)) {
                    fields[i].setAccessible(true);
                }
                final Object fo = fields[i].get(o);
                b = this.serialize(fields[i].getType().getName(), fo);
                if (b == null) {
                    b = new byte[]{};
                }
                if (Types.isObjType(fields[i])) {
                    if (fo == null) {
                        res.addBytesFromInt(0);
                    } else {
                        final byte[] cn = fo.getClass().getName().getBytes("UTF-8");
                        res.addBytesFromInt(cn.length);
                        res.append(cn);
                    }
                }
                if (Types.isVarType(fields[i])) {
                    res.addBytesFromInt(b.length);
                    res.append(b);
                } else {
                    res.append(b);
                }
            }
        }
        return res.getBytes();
    }

    private byte[] substring(byte[] b, int startPos, int endPos) {
        byte[] res = new byte[endPos-startPos];
        System.arraycopy(b, startPos, res, 0, endPos-startPos);
        return res;
    }

    private byte[] append(byte[] b, byte[] toAdd) {
        byte[] res = new byte[b.length + toAdd.length];
        System.arraycopy(b, 0, res, 0, b.length);
        System.arraycopy(toAdd, 0, res, b.length, toAdd.length);
        return res;
    }

    private byte[] append(byte[] b, byte[] toAdd1, byte[] toAdd2) {
        byte[] res = new byte[b.length + toAdd1.length + toAdd2.length];
        System.arraycopy(b, 0, res, 0, b.length);
        System.arraycopy(toAdd1, 0, res, b.length, toAdd1.length);
        System.arraycopy(toAdd2, 0, res, b.length+toAdd1.length, toAdd2.length);
        return res;
    }

}
