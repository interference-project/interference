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

import java.nio.ByteBuffer;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class RowId implements Comparable {

    private volatile int file;
    private volatile long frame;
    private final int row;

    public RowId (int file, long frame, int row) {
        this.file = file;
        this.frame = frame;
        this.row = row;
    }

    public RowId (RowId rowid) {
        this.file = rowid.getFileId();
        this.frame  = rowid.getFramePointer();
        this.row  = rowid.getRowPointer();
    }

    public RowId (byte[] rowid) {
        this.file = getIntFromBytes(substring(rowid,0,4));
        this.frame  = getLongFromBytes(substring(rowid,4,12));
        this.row  = getIntFromBytes(substring(rowid,12,16));
    }

    public boolean equals (RowId r) {
        if ((this.getFileId() == r.getFileId())&&(this.getFramePointer() == r.getFramePointer())&&(this.getRowPointer() == r.getRowPointer())) {
            return true;
        }
        return false;
    }

    public int compareTo (Object o) {
        RowId r = (RowId)o;
        if (this.file < r.getFileId()) { return -1; } else if (this.file > r.getFileId()) { return 1; }
        if (this.frame < r.getFramePointer()) { return -1; } else if (this.frame > r.getFramePointer()) { return 1; }
        if (this.row < r.getRowPointer()) { return -1; } else if (this.row > r.getRowPointer()) { return 1; }
        return 0;
    }

    public byte[] getBytes () {
        byte[] res = new byte[]{};
        res = append(res, getBytesFromInt(getFileId()));
        res = append(res, getBytesFromLong(getFramePointer()));
        res = append(res, getBytesFromInt(getRowPointer()));
        return res;
    }

    public String getRowidAsString () {
        return "" + this.file + ":" + this.frame + ":" + this.row;
    }

    public int getFileId () {
        return this.file;
    }

    public long getFramePointer() {
        return this.frame;
    }

    public int getRowPointer() {
        return this.row;
    }

    public void setFileId(int file) {
        this.file = file;
    }

    public void setFramePointer(long frame) {
        this.frame = frame;
    }

    /************************************************************
     byte utils
     ***********************************************************/

    public byte[] getBytesFromInt (int p) {
        byte[] res = new byte[4];
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(p);
        res = bb.array();
        return res;
    }

    public byte[] getBytesFromLong (long p) {
        byte[] res = new byte[8];
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(p);
        res = bb.array();
        return res;
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

    public static int getIntByHex (String hexval) {
        return Integer.parseInt(hexval.trim(),16);
    }

    public static long getLongByHex (String hexval) {
        return Long.parseLong(hexval.trim(),16);
    }

    public static String getHexByInt (int intval) {
       String s = Integer.toHexString(intval);
       String p = new String();
       for (int i=0; i < 8-s.length(); i++) {
           p = p + "0";
       }
       return p + s;
    }

    public static String getHexByLong (long longval) {
       String s = Long.toHexString(longval);
       String p = new String();
       for (int i=0; i < 16-s.length(); i++) {
           p = p + "0";
       }
       return p + s;
    }

    public static RowId getRowIdFromHex (String hex) {
        RowId res = new RowId(getIntByHex(hex.substring(0,8)),getLongByHex(hex.substring(8,24)),getIntByHex(hex.substring(24,32)));
        return res;
    }

    public static String getHexFromRowId (RowId r) {
        String res = new String();
        res = res + getHexByInt(r.getFileId());
        res = res + getHexByLong(r.getFramePointer());
        res = res + getHexByInt(r.getRowPointer());
        return res;
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

}
