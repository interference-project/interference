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

package su.interference.core;

import su.interference.exception.InternalException;
import su.interference.persistent.Table;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
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

public class ByteArray {

    private byte[] b = new byte[]{};

    public ByteArray(String t, Object fo) throws IllegalAccessException, UnsupportedEncodingException, ClassNotFoundException, InternalException, InstantiationException {
        b = getBytes(t, fo);
    }

    public ByteArray(byte[] b) {
        this.b = b;
    }

    public byte[] getBytes() {
        return b;
    }

    public byte[] getBytes(String t, Object fo) throws IllegalAccessException, UnsupportedEncodingException, ClassNotFoundException, InternalException, InstantiationException {
        if (!Types.isPrimitiveType(t)) {
            if (t.equals("java.lang.Integer")) {
                return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromInt(fo==null?0:(Integer)fo));
            }
            if (t.equals("java.lang.Long")) {
                return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromLong(fo==null?0:(Long)fo));
            }
            if (t.equals("java.util.concurrent.atomic.AtomicInteger")) {
                return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromInt(fo==null?0:((AtomicInteger)fo).get()));
            }
            if (t.equals("java.util.concurrent.atomic.AtomicLong")) {
                return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromLong(fo==null?0:((AtomicLong)fo).get()));
            }
            if (t.equals("java.lang.Float")) {
                return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromFloat(fo==null?0:(Float)fo));
            }
            if (t.equals("java.lang.Double")) {
                return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromDouble(fo==null?0:(Double)fo));
            }
            if (t.equals("java.util.Date")) {
                return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromDate((Date)fo));
            }
            if (t.equals("java.lang.String")) {
                return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromString((String)fo));
            }
            if (t.equals("java.util.ArrayList")) {
                return append(fo==null?new byte[]{0}:new byte[]{1},getBytesFromArrayList((ArrayList)fo));
            }
            if (t.equals("[B")) {
                return append(fo==null?new byte[]{0}:new byte[]{1},(byte[])fo);
            }
            if (t.equals("su.interference.core.DataChunk")) {
                return append(fo==null?new byte[]{0}:new byte[]{1}, append(getBytesFromInt(((DataChunk)fo).getT().getObjectId()),
                        append(getBytesFromInt(((DataChunk)fo).getHeader().getRowID().getFileId()), getBytesFromLong(((DataChunk)fo).getHeader().getRowID().getFramePointer())),
                        append(((DataChunk)fo).getHeader().getHeader(), ((DataChunk)fo).getChunk())));
            }
        } else {
            if (t.equals("byte")) {
                byte[] res = new byte[1];
                res[0] = (Byte)fo;
                return res;
            }
            if (t.equals("char")) {
                return getBytesFromChar((Character)fo);
            }
            if (t.equals("int")) {
                return getBytesFromInt((Integer)fo);
            }
            if (t.equals("long")) {
                return getBytesFromLong((Long)fo);
            }
            if (t.equals("float")) {
                return getBytesFromFloat((Float)fo);
            }
            if (t.equals("double")) {
                return getBytesFromDouble((Double)fo);
            }
        }
        return null;
    }

    public Object getFieldValue (Field f) throws UnsupportedEncodingException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException {
        String t = f.getType().getName();
        String g = f.getGenericType().toString();
        return getFieldValue(b, t, g);
    }

    public Object getFieldValue (byte[] b, String t, String g) throws UnsupportedEncodingException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException {
        if (!Types.isPrimitiveType(t)) {
            //for notnull types, 1-byte length value equals null object and must contains 0 in digit equivalent
            int isNull = getIntFromBytes(substring(b,0,1));
            if (isNull==0) {
                return null;
            }
            if (t.equals("java.lang.Integer")) {
                return new Integer(getIntFromBytes(substring(b,1,b.length)));
            }
            if (t.equals("java.lang.Long")) {
                return new Long(getLongFromBytes(substring(b,1,b.length)));
            }
            if (t.equals("java.util.concurrent.atomic.AtomicInteger")) {
                return new AtomicInteger(getIntFromBytes(substring(b,1,b.length)));
            }
            if (t.equals("java.util.concurrent.atomic.AtomicLong")) {
                return new AtomicLong(getLongFromBytes(substring(b,1,b.length)));
            }
            if (t.equals("java.lang.Float")) {
                return new Float(getFloatFromBytes(substring(b,1,b.length)));
            }
            if (t.equals("java.lang.Double")) {
                return new Double(getDoubleFromBytes(substring(b,1,b.length)));
            }
            if (t.equals("java.lang.Byte")) {
                return new Byte(b[1]);
            }
            if (t.equals("java.util.Date")) {
                return getDateFromBytes(substring(b,1,b.length));
            }
            if (t.equals("java.lang.String")) {
                return getStringFromBytes(substring(b,1,b.length));
            }
            if (t.equals("java.util.ArrayList")) {
                return getArrayListFromBytes(substring(b,1,b.length), g);
            }
            if (t.equals("[B")) {
                return substring(b,1,b.length);
            }
            if (t.equals("su.interference.core.DataChunk")) {
                Table to = Instance.getInstance().getTableById(getIntFromBytes(substring(b,1,5)));
                int file = getIntFromBytes(substring(b,5,9));
                long frame = getLongFromBytes(substring(b,9,17));
                RowHeader h = new RowHeader(substring(b,17,33),file,frame);
                return new DataChunk(substring(b,5,b.length), to, h, null);
            }
        } else {
            if (t.equals("byte")) {
                return b[0];
            }
            if (t.equals("int")) {
                return getIntFromBytes(b);
            }
            if (t.equals("long")) {
                return getLongFromBytes(b);
            }
            if (t.equals("float")) {
                return getFloatFromBytes(b);
            }
            if (t.equals("double")) {
                return getDoubleFromBytes(b);
            }
            if (t.equals("char")) {
                return getCharFromBytes(b);
            }
        }
        return null;
    }

    // get value from string
    public Object getFieldValue (String v, String t) throws UnsupportedEncodingException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException, ParseException {
        if (!Types.isPrimitiveType(t)) {
            if (v.length()==0) {
                return null;
            }
            if (t.equals("java.lang.Integer")) {
                return Integer.parseInt(v);
            }
            if (t.equals("java.lang.Long")) {
                return Long.parseLong(v);
            }
            if (t.equals("java.util.concurrent.atomic.AtomicInteger")) {
                return new AtomicInteger(Integer.parseInt(v));
            }
            if (t.equals("java.util.concurrent.atomic.AtomicLong")) {
                return new AtomicLong(Long.parseLong(v));
            }
            if (t.equals("java.lang.Float")) {
                return Float.parseFloat(v);
            }
            if (t.equals("java.lang.Double")) {
                return Double.parseDouble(v);
            }
            if (t.equals("java.util.Date")) {
                SimpleDateFormat df = new SimpleDateFormat(Instance.getInstance().getDateFormat());
                return df.parse(v);
            }
            if (t.equals("java.lang.String")) {
                return v;
            }
        } else {
            if (t.equals("int")) {
                return Integer.parseInt(v);
            }
            if (t.equals("long")) {
                return Long.parseLong(v);
            }
            if (t.equals("float")) {
                return Float.parseFloat(v);
            }
            if (t.equals("double")) {
                return Double.parseDouble(v);
            }
        }
        return null;
    }

    public byte[] getBytesFromInt (int p) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(p);
        return bb.array();
    }

    public byte[] getBytesFromLong (long p) {
        byte[] res = new byte[8];
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(p);
        res = bb.array();
        return res;
    }

    public byte[] getBytesFromFloat (float p) {
        byte[] res = new byte[4];
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putFloat(p);
        res = bb.array();
        return res;
    }

    public byte[] getBytesFromDouble (double p) {
        byte[] res = new byte[8];
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putDouble(p);
        res = bb.array();
        return res;
    }

    public byte[] getBytesFromChar (char p) {
        byte[] res = new byte[2];
        ByteBuffer bb = ByteBuffer.allocate(2);
        bb.putChar(p);
        res = bb.array();
        return res;
    }

    public byte[] getBytesFromString (String p) throws UnsupportedEncodingException, ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        byte[] res;
        if (p!=null) {
            res = p.getBytes(Instance.getInstance().getCodePage());
        } else {
            res = new byte[]{};
        }
        return res;
    }

    public byte[] getBytesFromArrayList (ArrayList p) throws UnsupportedEncodingException, IllegalAccessException, ClassNotFoundException, InternalException, InstantiationException {
        byte[] res = new byte[]{};
        String cn;
        if (p!=null) {
            if (p.size()>0) {
                cn = p.get(0).getClass().getName();
                for (Object o : p) {
                    byte[] b = getBytes(cn, o);
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

    public byte[] getBytesFromDate (Date p) {
        if (p!=null) {
            return getBytesFromLong(p.getTime());
        } else {
            return getBytesFromLong(0);
        }
    }

    public int getIntFromBytes(byte[] b) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(b);
        bb.rewind();
        return bb.getInt();
    }

    public long getLongFromBytes(byte[] b) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.put(b);
        bb.rewind();
        return bb.getLong();
    }

    public float getFloatFromBytes(byte[] b) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(b);
        bb.rewind();
        return bb.getFloat();
    }

    public double getDoubleFromBytes(byte[] b) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.put(b);
        bb.rewind();
        return bb.getDouble();
    }

    public char getCharFromBytes(byte[] b) {
        ByteBuffer bb = ByteBuffer.allocate(2);
        bb.put(b);
        bb.rewind();
        return bb.getChar();
    }

    public String getStringFromBytes (byte[] b) throws UnsupportedEncodingException, ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        return new String(b,Instance.getInstance().getCodePage());
    }

    public ArrayList getArrayListFromBytes (byte[] b, String t) throws UnsupportedEncodingException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException {
        ArrayList r = new ArrayList();
        if (b!=null) {
            if (b.length>0) {
                if (t.indexOf("<")>=0&&t.indexOf(">")>0) {
                    String et = t.substring(t.indexOf("<")+1,t.lastIndexOf(">"));
                    //for cases like inline Lists (ArrayList<ArrayList<...>>)
                    //et must contain only type name (java.util.ArrayList)
                    if (et.indexOf("<")>=0) {
                        et = et.substring(0, et.indexOf("<"));
                    }
                    int v = Types.isVarType(et)?4:0;
                    int s = 0;
                    boolean cnue = true;
                    while (cnue) {
                        byte[] data = substring(b, s+v, s+v+(Types.isVarType(et)?getIntFromBytes(substring(b,s,s+v)):Types.getTypeLength(et,0)));
                        s = s + data.length + v;
                        r.add(getFieldValue(data, et, et));
                        if (s>=b.length) { cnue=false; }
                    }
                }
            }
        }
        return r;
    }

    public Date getDateFromBytes (byte[] b) {
        Date d = new Date();
        d.setTime(getLongFromBytes(b));
        return d;
    }

    public byte[] substring(byte[] b, int startPos, int endPos){
        byte[] res = new byte[endPos-startPos];
        System.arraycopy(b, startPos, res, 0, endPos-startPos);
        return res;
    }

    public byte[] append(byte[] b, byte[] toAdd){
        byte[] res = new byte[b.length + toAdd.length];
        System.arraycopy(b, 0, res, 0, b.length);
        System.arraycopy(toAdd, 0, res, b.length, toAdd.length);
        return res;
    }

    public byte[] append(byte[] b, byte[] toAdd1, byte[] toAdd2){
        byte[] res = new byte[b.length + toAdd1.length + toAdd2.length];
        System.arraycopy(b, 0, res, 0, b.length);
        System.arraycopy(toAdd1, 0, res, b.length, toAdd1.length);
        System.arraycopy(toAdd2, 0, res, b.length+toAdd1.length, toAdd2.length);
        return res;
    }

    public byte[] append(byte[] b, byte toAdd){
        byte[] res = new byte[b.length + 1];
        byte[] ta = new byte[1];
        ta[0] = toAdd;
        System.arraycopy(b, 0, res, 0, b.length);
        System.arraycopy(ta, 0, res, b.length, 1);
        return res;
    }

    public byte[] append(byte b, byte[] toAdd){
        byte[] res = new byte[toAdd.length + 1];
        byte[] first = new byte[1];
        first[0] = b;
        System.arraycopy(first, 0, res, 0, first.length);
        System.arraycopy(toAdd, 0, res, first.length, toAdd.length);
        return res;
    }

    public byte[] substring(int startPos, int endPos){
        byte[] res = new byte[endPos-startPos];
        System.arraycopy(b, startPos, res, 0, endPos-startPos);
        return res;
    }

    public byte[] append(byte[] toAdd){
        byte[] res = new byte[b.length + toAdd.length];
        System.arraycopy(b, 0, res, 0, b.length);
        System.arraycopy(toAdd, 0, res, b.length, toAdd.length);
        return res;
    }

    public byte[] append(byte toAdd){
        byte[] res = new byte[b.length + 1];
        byte[] ta = new byte[1];
        ta[0] = toAdd;
        System.arraycopy(b, 0, res, 0, b.length);
        System.arraycopy(ta, 0, res, b.length, 1);
        return res;
    }

}
