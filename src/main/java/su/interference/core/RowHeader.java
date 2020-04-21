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

import su.interference.persistent.Transaction;

import java.nio.ByteBuffer;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class RowHeader implements Header, Comparable {

    private volatile RowId rowID;  // not stored in physical header
    private volatile RowId framePtr; // used in standalone indexes
    private Transaction tran;      // not stored in physical header
    private volatile long transId;
    private volatile int state;
    private volatile int len;
    private volatile int ptr;
    private volatile int res;
    private volatile long lltId;

    // UPDATE operation
    public RowHeader (byte[] h, int file, long frame) {
        setTransId(getLongFromBytes(substring(h,0,8)));
        setTran(Instance.getInstance().getTransactionById(transId));
        setState(getIntFromBytes(append(new byte[2],substring(h,8,10))));
        setLen(getIntFromBytes(append(new byte[2],substring(h,10,12))));
        setPtr(getIntFromBytes(append(new byte[2],substring(h,12,14))));
        setRes(getIntFromBytes(append(new byte[2],substring(h,14,16))));
        if (h.length == Frame.INDEX_HEADER_SIZE) {
            setFramePtr(new RowId(substring(h,16,32)));
        }
        setRowID(new RowId(file, frame, ptr));
    }

    public RowHeader (RowHeader rh) {
        setTran(rh.getTran());
        setState(rh.getState());
        setLen(rh.getLen());
        setPtr(rh.getPtr());
        setRes(rh.getRes());
        setRowID(new RowId(rh.getRowID().getFileId(), rh.getRowID().getFramePointer(), rh.getRowID().getRowPointer()));
    }

    // INSERT operation
    public RowHeader (int file, long frame, int ptr, Transaction tran, int len) {
        setRowID(new RowId(file, frame, ptr));
        setState(Header.RECORD_NORMAL_STATE);
        setTran(tran);
        setLen(len);
        setPtr(ptr);
    }

    // INSERT INDEX operation
    public RowHeader (RowId r, Transaction tran, int len, boolean bulk) {
        setFramePtr(r);
        setState(Header.RECORD_NORMAL_STATE);
        setTran(tran);
        setLen(len);
    }

    public byte[] getHeader() {
        byte[] res = getBytesFromLong(this.tran==null?0:this.tran.getTransId());
        res = append(res, substring(getBytesFromInt(this.state),2,4));
        res = append(res, substring(getBytesFromInt(this.len),2,4));
        res = append(res, substring(getBytesFromInt(this.ptr),2,4));
        res = append(res, substring(getBytesFromInt(this.res),2,4));
        if (framePtr!=null) {
            res = append(res,framePtr.getBytes());
        }
        return res;
    }

    protected int getHeaderSize() {
        if (framePtr == null) {
            return Frame.ROW_HEADER_SIZE;
        } else {
            return Frame.INDEX_HEADER_SIZE;
        }
    }

    public int compareTo (Object o) {
        int v1 = this.getLen();
        int v2 = ((RowHeader)o).getLen();
        if (v1==v2) {
            return 0;
        } else if (v1>v2) {
            return 1;
        } else if (v1<v2) {
            return -1;
        }
        return 0;
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

    public RowId getRowID() {
        return this.rowID;
    }

    public void setRowID(RowId rowID) {
        this.rowID = rowID;
    }

    public Transaction getTran() {
        if (tran==null) {
            Instance.getInstance().getTransactionById(transId);
        }
        return tran;
    }

    public void setTran(Transaction tran) {
        this.tran = tran;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }

    public int getPtr() {
        return ptr;
    }

    public void setPtr(int ptr) {
        this.ptr = ptr;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public int getRes() {
        return res;
    }

    public void setRes(int res) {
        this.res = res;
    }

    public RowId getFramePtrRowId() {
        return framePtr;
    }

    public long getFramePtr() {
        return framePtr.getFileId()+framePtr.getFramePointer();
    }

    public void setFramePtr(RowId framePtr) {
        this.framePtr = framePtr;
    }

    public long getLltId() {
        return lltId;
    }

    public void setLltId(long lltId) {
        this.lltId = lltId;
    }

    public long getTransId() {
        return transId;
    }

    public void setTransId(long transId) {
        this.transId = transId;
    }
}
