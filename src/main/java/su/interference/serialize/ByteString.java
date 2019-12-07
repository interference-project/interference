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

package su.interference.serialize;

import su.interference.core.DataChunk;
import su.interference.core.Instance;
import su.interference.core.RowHeader;
import su.interference.core.Types;
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

public class ByteString {

    private byte[] b;

    public ByteString(byte[] b) {
        this.b = b;
    }

    public ByteString() {
        this.b = new byte[]{};
    }

    public byte[] getBytes() {
        return b;
    }

    public byte[] getBytesFromInt (int p) {
        final ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(p);
        return bb.array();
    }

    public byte[] getBytesFromLong (long p) {
        final ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(p);
        return bb.array();
    }

    public void addBytesFromInt (int p) {
        final ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(p);
        append(bb.array());
    }

    public void addBytesFromLong (long p) {
        final ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(p);
        append(bb.array());
    }

    public int getIntFromBytes(byte[] b) {
        final ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(b);
        bb.rewind();
        return bb.getInt();
    }

    public long getLongFromBytes(byte[] b) {
        final ByteBuffer bb = ByteBuffer.allocate(8);
        bb.put(b);
        bb.rewind();
        return bb.getLong();
    }

    public int getIntFromBytes(int pos) {
        final byte[] b = substring(pos, pos+4);
        final ByteBuffer bb = ByteBuffer.allocate(4);
        bb.put(b);
        bb.rewind();
        return bb.getInt();
    }

    public long getLongFromBytes(int pos) {
        final byte[] b = substring(pos, pos+8);
        final ByteBuffer bb = ByteBuffer.allocate(8);
        bb.put(b);
        bb.rewind();
        return bb.getLong();
    }

    public byte[] substring(int start, int end){
        final byte[] res = new byte[end-start];
        System.arraycopy(b, start, res, 0, end-start);
        return res;
    }

    public void append(byte[] add){
        final byte[] res = new byte[b.length + add.length];
        System.arraycopy(b, 0, res, 0, b.length);
        System.arraycopy(add, 0, res, b.length, add.length);
        this.b = res;
    }

    public byte[] append(byte add){
        final byte[] res = new byte[b.length + 1];
        final byte[] a = new byte[1];
        a[0] = add;
        System.arraycopy(b, 0, res, 0, b.length);
        System.arraycopy(a, 0, res, b.length, 1);
        return res;
    }

}
