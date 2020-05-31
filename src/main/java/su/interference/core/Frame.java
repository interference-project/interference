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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.persistent.*;
import su.interference.exception.*;
import su.interference.serialize.ByteString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class Frame implements Comparable {

    private final int file;
    private final long pointer;
    private final int  allocFile;
    private final long allocPointer;
    private volatile int objectId;
    private volatile int type;
    private volatile int cptr;
    private volatile int bytesAmount;
    private volatile int rowCntr;
    private volatile int sptr;
    private volatile int res01;
    private volatile int res02;
    private volatile int res03;
    private volatile int res04;
    private volatile int res05;
    private volatile long res06;
    private volatile long res07;
    private volatile long res08;
    private volatile int res09;
    //non-persistent
    private final int frameSize;

    public static final int FRAME_HEADER_SIZE = 96;
    public static final int ROW_HEADER_SIZE   = 16;
    public static final int INDEX_HEADER_SIZE = 32;
    public static final int MIN_FRAME_SIZE    = 256; //test implementation only
    public static final int MAX_FRAME_SIZE    = 65535;
    public static final int MAX_PTR_VALUE     = Integer.MAX_VALUE;
    public static final int DATA_FRAME        = 1;
    public static final int INDEX_FRAME       = 2;

    protected final ChunkMap data = new ChunkMap(this);
    private final ConcurrentHashMap<Long, byte[]> snap = new ConcurrentHashMap<Long, byte[]>();
    protected byte[] b;

    private FrameData frameData;
    private Table dataObject;
    private Class entityClass;
    private static final Logger logger = LoggerFactory.getLogger(Frame.class);

    protected int getFrameSize() {
        return frameSize;
    }

    public FrameData getFrameData() {
        if (frameData == null) {
            frameData = Instance.getInstance().getFrameById(this.file + this.pointer);
        }
        return frameData;
    }

    public void setFrameData(FrameData frameData) {
        this.frameData = frameData;
    }

    public Table getDataObject() {
        return dataObject;
    }

    public Class getEntityClass() {
        return entityClass;
    }

    public Frame(int file, long pointer, int size, Table t) throws InternalException {
        this.file       = file;
        this.pointer    = pointer;
        this.allocFile  = 0;
        this.allocPointer = 0;
        this.rowCntr    = 1;
        this.dataObject = t;
        this.frameSize    = size;
        if (this.frameSize<MIN_FRAME_SIZE) {
            throw new InternalException();
        }    
        if (t!=null) {
            this.objectId = t.getObjectId();
        }
    }

    public Frame(FrameData bd, Table t) throws InternalException {
        this.file         = bd.getFile();
        this.pointer      = bd.getPtr();
        this.allocFile    = (int)bd.getAllocId()%4096;
        this.allocPointer = bd.getAllocId() - (bd.getAllocId()%4096);
        this.rowCntr      = 1;
        this.frameData    = bd;
        this.dataObject   = t;
        this.frameSize      = bd.getSize();
        if (this.frameSize<MIN_FRAME_SIZE) {
            throw new InternalException();
        }
        if (t!=null) {
            this.objectId = t.getObjectId();
        }
    }

    //constructor for retrieve frame from journal file
    public Frame(byte[] b) throws InternalException {
        if (b.length<FRAME_HEADER_SIZE) {
            throw new InvalidFrameHeader();
        }
        if (b.length<MIN_FRAME_SIZE) {
            throw new InternalException();
        }
        this.b = b;
        this.frameSize = b.length;
        final ByteString bs = new ByteString(b);
        this.file = bs.getIntFromBytes(0);
        this.pointer = bs.getLongFromBytes(4);
        this.objectId = bs.getIntFromBytes(12);
        this.type = bs.getIntFromBytes(16);
        this.cptr = bs.getIntFromBytes(20);
        this.bytesAmount = bs.getIntFromBytes(24);
        this.rowCntr = bs.getIntFromBytes(28);
        this.sptr  = bs.getIntFromBytes(32);
        this.allocFile = bs.getIntFromBytes(36);
        this.allocPointer = bs.getLongFromBytes(40);
        this.res01 = bs.getIntFromBytes(48);
        this.res02 = bs.getIntFromBytes(52);
        this.res03 = bs.getIntFromBytes(56);
        this.res04 = bs.getIntFromBytes(60);
        this.res05 = bs.getIntFromBytes(64);
        this.res06 = bs.getLongFromBytes(68);
        this.res07 = bs.getLongFromBytes(76);
        this.res08 = bs.getLongFromBytes(84);
        this.res09 = bs.getIntFromBytes(92);

        if ((this.file==0)&&(this.pointer==0)) {
            throw new EmptyFrameHeaderFound();
        }
        if (this.objectId<0) {
            throw new InvalidFrameHeader();
        }
        if (this.bytesAmount<FRAME_HEADER_SIZE) {
            throw new InvalidFrameHeader();
        }
        if (this.b.length<this.bytesAmount) {
            throw new InvalidFrame();
        }
    }

    //constructor for replication service
    public Frame(byte[] b, int file, long pointer, Table t) throws InternalException {
        this.dataObject = t;
        if (b.length<FRAME_HEADER_SIZE) {
            throw new InvalidFrameHeader();
        }
        if (b.length<MIN_FRAME_SIZE) {
            throw new InternalException();
        }
        this.b = b;
        this.frameSize = b.length;
        this.file = file;
        this.pointer = pointer;
        final ByteString bs = new ByteString(b);
        this.objectId = bs.getIntFromBytes(12);
        this.type = bs.getIntFromBytes(16);
        this.cptr = bs.getIntFromBytes(20);
        this.bytesAmount = bs.getIntFromBytes(24);
        this.rowCntr = bs.getIntFromBytes(28);
        this.sptr  = bs.getIntFromBytes(32);
        this.allocFile = bs.getIntFromBytes(36);
        this.allocPointer = bs.getLongFromBytes(40);
        this.res01 = bs.getIntFromBytes(48);
        this.res02 = bs.getIntFromBytes(52);
        this.res03 = bs.getIntFromBytes(56);
        this.res04 = bs.getIntFromBytes(60);
        this.res05 = bs.getIntFromBytes(64);
        this.res06 = bs.getLongFromBytes(68);
        this.res07 = bs.getLongFromBytes(76);
        this.res08 = bs.getLongFromBytes(84);
        this.res09 = bs.getIntFromBytes(92);

        if ((this.file==0)&&(this.pointer==0)) {
            throw new EmptyFrameHeaderFound();
        }
        if (this.objectId<0) {
            throw new InvalidFrameHeader();
        }
        if (this.bytesAmount<FRAME_HEADER_SIZE) {
            throw new InvalidFrameHeader();
        }
        if (this.b.length<this.bytesAmount) {
            throw new InvalidFrame();
        }
    }

    public Frame(byte[] bb, int file, long pointer, int size, FrameData bd, Table t, Class c) throws IOException, InternalException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        entityClass = c;
        dataObject = t;
        if (bd!=null) { //system frame not used dataobject
            this.frameData = bd;
            this.dataObject = dataObject==null?bd.getDataObject():dataObject;
        }
        if (bb==null) {
            final DataFile df = Storage.getStorage().getDataFileById(file);
            this.b = df.readData(pointer, bd==null?size:bd.getSize());
        } else {
            this.b = bb;
        }
//        this.dataObject = t;
        if (this.b.length<FRAME_HEADER_SIZE) {
            throw new InvalidFrameHeader();
        }
        if (this.b.length<MIN_FRAME_SIZE) {
            throw new InternalException();
        }
        this.frameSize = b.length;
        final ByteString bs = new ByteString(b);
        this.file = bs.getIntFromBytes(0);
        this.pointer = bs.getLongFromBytes(4);
        this.objectId = bs.getIntFromBytes(12);
        this.type = bs.getIntFromBytes(16);
        this.cptr = bs.getIntFromBytes(20);
        this.bytesAmount = bs.getIntFromBytes(24);
        this.rowCntr = bs.getIntFromBytes(28);
        this.sptr  = bs.getIntFromBytes(32);
        this.allocFile = bs.getIntFromBytes(36);
        this.allocPointer = bs.getLongFromBytes(40);
        this.res01 = bs.getIntFromBytes(48);
        this.res02 = bs.getIntFromBytes(52);
        this.res03 = bs.getIntFromBytes(56);
        this.res04 = bs.getIntFromBytes(60);
        this.res05 = bs.getIntFromBytes(64);
        this.res06 = bs.getLongFromBytes(68);
        this.res07 = bs.getLongFromBytes(76);
        this.res08 = bs.getLongFromBytes(84);
        this.res09 = bs.getIntFromBytes(92);

        if ((this.file==0)&&(this.pointer==0)) {
            logger.error("empty frame header frameId = " + (bd == null ? "N/A" : bd.getFrameId()) + " allocId = " + (bd == null ? "N/A" : bd.getAllocId()));
            throw new EmptyFrameHeaderFound();
        }
        if ((this.file!=file)||(this.pointer!=pointer)) {
            logger.error("invalid frame header frameId = " + (bd == null ? "N/A" : bd.getFrameId()) + " allocId = " + (bd == null ? "N/A" : bd.getAllocId()));
            throw new InvalidFrameHeader();
        }
        if (this.objectId<0) {
            throw new InvalidFrameHeader();
        }
        if (this.bytesAmount<FRAME_HEADER_SIZE) {
            throw new InvalidFrameHeader();
        }
        if (this.b.length<this.bytesAmount) {
            throw new InvalidFrame();
        }
    }

    public synchronized byte[] getFrame() throws InvalidFrame {
        final ByteString res = new ByteString();
        final ByteString res2 = new ByteString();
        int used = FRAME_HEADER_SIZE;
        final long sync = LLT.getSyncId();

        if (Config.getConfig().SYNC_LOCK_ENABLE||sync==0) {
            for (Chunk c : data.getChunks()) {
                final byte[] chunk_ = c.getChunk();
                c.getHeader().setLen(chunk_.length);
                res2.append(c.getHeader().getHeader());
                res2.append(chunk_);
                used = used + c.getBytesAmount();
            }
        } else {
            if (sync==0) {
                throw new InvalidFrame();
            }
            for (Chunk c : data.getChunks()) {
                if (c.getHeader().getLltId() < sync) {
                    final byte[] chunk_ = c.getChunk();
//                    c.getHeader().setLen(chunk_.length);
                    res2.append(c.getHeader().getHeader());
                    res2.append(chunk_);
                    used = used + c.getBytesAmount();
                }
            }
            for (Map.Entry<Long, byte[]> entry : snap.entrySet()) {
                if (entry.getKey() > sync) {
                    res2.append(entry.getValue());
                    used = used + entry.getValue().length;
                }
            }
        }

        this.bytesAmount = used;

        res.addBytesFromInt(this.file);
        res.addBytesFromLong(this.pointer);
        res.addBytesFromInt(this.objectId);
        res.addBytesFromInt(this.type);
        res.addBytesFromInt(this.cptr);
        res.addBytesFromInt(this.bytesAmount);
        res.addBytesFromInt(this.rowCntr);
        res.addBytesFromInt(this.sptr);
        res.addBytesFromInt(this.allocFile);
        res.addBytesFromLong(this.allocPointer);
        res.addBytesFromInt(this.res01);
        res.addBytesFromInt(this.res02);
        res.addBytesFromInt(this.res03);
        res.addBytesFromInt(this.res04);
        res.addBytesFromInt(this.res05);
        res.addBytesFromLong(this.res06);
        res.addBytesFromLong(this.res07);
        res.addBytesFromLong(this.res08);
        res.addBytesFromInt(this.res09);
        res.append(res2.getBytes());
        
        if (used > getFrameSize()) {
            logger.error("Build snapshot for "+this.getClass().getName()+":"+this.getObjectId()+":"+this.pointer+" with used length="+used+" failed, not enough size for expected framesize="+getFrameSize()+"");
            throw new InvalidFrame();
        }
        if (used < getFrameSize()) {
            res.append(new byte[getFrameSize()-used]);
        }
        return res.getBytes();
    }

    public boolean equals (Frame bl) {
        if ((this.file==bl.getFile())&&(this.pointer==bl.getPointer())) {
            return true;
        } else {
            return false;
        }
    }

    public int compareTo(Object obj) {
        Frame b = (Frame)obj;
        if (this.pointer < b.getPointer()) {
            return -1;
        }
        if (this.pointer > b.getPointer()) {
            return 1;
        }
        return 0;
    }

    public int getFrameFree () {
        return getFrameSize() - getBytesAmount();
    }

    protected void clearSnaps(long id) {
        for (Map.Entry<Long, byte[]> entry : snap.entrySet()) {
            if (entry.getKey() == id) {
                snap.remove(entry.getKey());
            }
        }
    }

    //insert method for new chunks without header
    public synchronized int insertChunk (Chunk c, Session s, boolean check, LLT llt) {
        final Transaction tran = s.getTransaction();

        final int ptr = this.getRowCntr();

        //todo need fix timelapse bomb, need some row pointers recycle
        if (ptr >= Frame.MAX_PTR_VALUE) {
            throw new RuntimeException("row pointers are over, everything is lost");
        }

        if (check) {
            final int frameFree = this.getFrameFree();
            final int chunkSize = c.getBytesAmount();
            if (frameFree >= chunkSize) {
                this.setRowCntr(ptr+1);
                c.getHeader().setRowID(new RowId(this.getFile(), this.getPointer(), ptr));
                c.getHeader().setPtr(ptr);
                c.getHeader().setTran(tran);
                c.getHeader().setLltId(llt==null?0:llt.getId());
                if (llt != null) { llt.add(this); }
                data.add(c);
                return ptr;
            } else {
                return 0;
            }
        } else {
            this.setRowCntr(ptr+1);
            c.getHeader().setRowID(new RowId(this.getFile(), this.getPointer(), ptr));
            c.getHeader().setPtr(ptr);
            c.getHeader().setTran(tran);
            c.getHeader().setLltId(llt==null?0:llt.getId());
            if (llt!=null) { llt.add(this); }
            data.add(c);
            return ptr;
        }

    }

    public synchronized int updateChunk(DataChunk chunk, Object o, Session s, LLT llt) throws ClassNotFoundException, InvocationTargetException, IOException, NoSuchMethodException, InternalException, IllegalAccessException, InstantiationException {
        final long sync = LLT.getSyncId();
        if (chunk.getHeader().getLltId() < sync) {
            final ByteString sc = new ByteString();
            sc.append(chunk.getHeader().getHeader());
            sc.append(chunk.getChunk());
            if (!Config.getConfig().SYNC_LOCK_ENABLE) { snap.put(llt.getId(), sc.getBytes()); }
            chunk.getHeader().setLltId(llt==null?0:llt.getId());
        }
        if (llt!=null) { llt.add(this); }
        if (chunk.getEntity()!=o) { //not same object
            chunk.updateEntity(o);
        }
        chunk.setNormalState();
        final byte[] ncb = chunk.getChunk();
        chunk.getHeader().setLen(ncb.length);
        final int newlen = chunk.getBytesAmount();
        return newlen;
    }

    public synchronized void deleteChunk (int ptr, Session s, LLT llt) throws CannotAccessToLockedRecord, CannotAccessToDeletedRecord {
        final long sync = LLT.getSyncId();
        final Chunk chunk = getChunkByPtr(ptr);
        if (chunk==null) { throw new CannotAccessToDeletedRecord(); }
        final Header header = chunk.getHeader();
        if (header.getState()==Header.RECORD_LOCKED_STATE) { throw new CannotAccessToLockedRecord(); }
        if (header.getLltId() < sync) {
            final ByteString sc = new ByteString();
            sc.append(chunk.getHeader().getHeader());
            sc.append(chunk.getChunk());
            if (!Config.getConfig().SYNC_LOCK_ENABLE) { snap.put(llt.getId(), sc.getBytes()); }
            header.setLltId(llt==null?0:llt.getId());
        }
        if (llt!=null) { llt.add(this); }
        header.setState(Header.RECORD_DELETED_STATE);
    }

    public synchronized List<Chunk> getChunks() {
        return data.getChunks();
    }

    public Chunk getChunkByPtr(int ptr) {
        return data.getByPtr(ptr);
    }

    public synchronized void removeChunk (int ptr, LLT llt, boolean ignore) {
        final long sync = LLT.getSyncId();
        final Chunk chunk = data.getByPtr(ptr);
        if (chunk == null) {
            if (!ignore) {
                throw new NullPointerException();
            }
        } else {
            if (chunk.getHeader().getLltId() < sync) {
                final ByteString sc = new ByteString();
                sc.append(chunk.getHeader().getHeader());
                sc.append(chunk.getChunk());
                if (!Config.getConfig().SYNC_LOCK_ENABLE) { snap.put(llt.getId(), sc.getBytes()); }
                chunk.getHeader().setLltId(llt == null ? 0 : llt.getId());
            }
            if (llt != null) { llt.add(this); }
            data.removeByPtr(ptr);
        }
    }

    //returns all actual records
    public synchronized ArrayList<Chunk> getFrameChunks (Session s) throws IOException, ClassNotFoundException {
        final ArrayList<Chunk> res = new ArrayList<Chunk>();

        if (dataObject==null) {
            dataObject = Instance.getInstance().getTableById(this.objectId);
        }
        if (dataObject.isNoTran() || s.isStream()) {

            final int streamptr = s.isStream() ? s.streamFramePtr(this) : 0;

            for (Chunk c : data.getChunks()) {
                if (c.getHeader().getState()==Header.RECORD_NORMAL_STATE) {
                    if (c.getHeader().getPtr() >= streamptr) {
                        res.add(c);
                    }
                }
            }

            if (s.isStream()) {
                s.streamFramePtr(this, rowCntr);
            }

        } else { //if (local()) {

            final long tr = s.getTransaction().getTransId();
            final long mtran = s.getTransaction().getMTran();

            // read frame records
            for (Chunk c : data.getChunks()) {
                Header h = c.getHeader();
                if (h.getState()==Header.RECORD_NORMAL_STATE) {
                    if (c.getUndoChunk()!=null) { //updated chunk
                        res.add(c);
                    } else {
                        if (h.getTran()==null) {
                            res.add(c);
                        } else {
                            if ((tr==h.getTran().getTransId())||(h.getTran().getCid()>0&&h.getTran().getCid()<=mtran)) {
                                res.add(c);
                            }
                        }
                    }
                }
                if (h.getState()==Header.RECORD_DELETED_STATE) {
                    if (h.getTran()!=null) {
                        if ((tr!=h.getTran().getTransId())&&(h.getTran().getCid()==0||h.getTran().getCid()>mtran)) {
                            res.add(c);
                        }
                    }
                }
            }
/*
        } else {
            // read frame records
            for (Chunk c : data.getChunks()) {
                Header h = c.getHeader();
                if (h.getState()==Header.RECORD_NORMAL_STATE) {
                    if (c.getUndoChunk()!=null) { //updated chunk
                        res.add(c);
                    } else {
                        if (h.getTran()==null) {
                            res.add(c);
                        } else {
                            if (h.getTran().getTransType()>=Transaction.TRAN_THR) {
                                res.add(c);
                            }
                        }
                    }
                }
                if (h.getState()==Header.RECORD_DELETED_STATE) {
                    if (h.getTran()!=null) {
                        if (h.getTran().getTransType()<Transaction.TRAN_THR) {
                            res.add(c);
                        }
                    }
                }
            }
*/
        }

        return res;
    }

    public synchronized void rollbackTransaction(Transaction tran, ArrayList<FrameData> ubs, Session s) throws InterruptedException {
        //rollback inserted records
        final ArrayList<Integer> r = new ArrayList<Integer>();
        for (Chunk c : data.getChunks()) {
            if (c.getHeader().getTran().getTransId()==tran.getTransId()) {
//                data.remove(entry.getKey());
                r.add(c.getHeader().getPtr());
            }
        }
        for (Integer i : r) {
            data.removeByPtr(i);
            //logger.debug("remove data "+this.file+" "+this.pointer+" "+i);
        }

        //rollback deleted & updated records
        if (ubs!=null) {
            try {
                for (FrameData ub : ubs) {
                    for (Chunk c : ub.getDataFrame().getChunks()) {
                        final UndoChunk uc = (UndoChunk)c.getEntity();
                        final long frameptr = uc.getDataChunk().getHeader().getRowID().getFramePointer();
                        //final int rowptr = uc.getDataChunk().getHeader().getRowID().getRowPointer();
                        if (uc.getTransId() == tran.getTransId() && uc.getFile() == this.file && frameptr == this.pointer) {
                            data.add(uc.getDataChunk().restore(this, s));
                            //logger.info("cb "+this.file+" "+this.pointer+" ub "+ub.getFile()+" "+ub.getPtr()+" restored "+uc.getDataChunk().getHeader().getRowID().getFileId()+" "+uc.getDataChunk().getHeader().getRowID().getFramePointer()+" "+uc.getDataChunk().getHeader().getRowID().getRowPointer()+" : "+uc.getFile()+" "+uc.getFrame()+" "+uc.getPtr());
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        final LLT llt = LLT.getLLT();
        llt.add(this);
        llt.commit();
    }

    public HashMap getLiveTransactions() {
        final HashMap rtran = new HashMap<Long, Transaction>();
        for (Chunk c : data.getChunks()) {
            final Transaction tran = c.getHeader().getTran();
            if (tran != null && tran.getTransType() < Transaction.TRAN_THR) {
                rtran.put(tran.getTransId(), tran);
            }
        }
        return rtran;
    }

    public int getBytesAmount() {
        int used = FRAME_HEADER_SIZE;
        final int size = data.getChunks().size();
        for (int i=0; i<size; i++) {
            used = used + data.getChunks().get(i).getBytesAmount();
        }
        return used;
    }

    public int getBytesAmountWoHead() {
        int used = 0;
        for (Chunk c : data.getChunks()) {
            used = used + c.getBytesAmount();
        }
        return used;
    }

    public boolean isLocal() {
        return this.file+this.pointer == this.allocFile+this.allocPointer;
    }

    public long getPtr() {
        return pointer+file;
    }

    public int getFile() {
        return file;
    }

    public int getAllocFile() {
        return allocFile;
    }

    public long getAllocPointer() {
        return allocPointer;
    }

    public long getPointer() {
        return pointer;
    }

    public int getObjectId() {
        return objectId;
    }

    public void setObjectId(int objectId) {
        this.objectId = objectId;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getRowCntr() {
        return rowCntr;
    }

    public void setRowCntr(int rowCntr) {
        this.rowCntr = rowCntr;
    }

    public int getRes01() {
        return res01;
    }

    public void setRes01(int res01) {
        this.res01 = res01;
    }

    public int getRes02() {
        return res02;
    }

    public void setRes02(int res02) {
        this.res02 = res02;
    }

    public int getRes03() {
        return res03;
    }

    public void setRes03(int res03) {
        this.res03 = res03;
    }

    public int getRes04() {
        return res04;
    }

    public void setRes04(int res04) {
        this.res04 = res04;
    }

    public int getRes05() {
        return res05;
    }

    public void setRes05(int res05) {
        this.res05 = res05;
    }

    public long getRes06() {
        return res06;
    }

    public void setRes06(long res06) {
        this.res06 = res06;
    }

    public long getRes07() {
        return res07;
    }

    public void setRes07(long res07) {
        this.res07 = res07;
    }

    public long getRes08() {
        return res08;
    }

    public void setRes08(long res08) {
        this.res08 = res08;
    }


}
