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

import su.interference.exception.*;
import su.interference.persistent.*;
import su.interference.serialize.ByteString;

import java.util.*;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class IndexFrame extends Frame {
    private boolean sorted = false;
    public static final int INDEX_FRAME_NODE = 2;
    public static final int INDEX_FRAME_LEAF = 1;
    public static final int INITIALIZE_DURING_CONSTRUCT = 1;

    public IndexFrame(int file, long pointer, int size, int objectId, Table t) throws InternalException {
        super(file, pointer, size, t);
    }

    public IndexFrame(FrameData bd, int frameType, Table t) throws InternalException {
        super(bd, t);
        this.setType(frameType);
    }

    public IndexFrame(int file, long pointer, int size, FrameData bd, Table t, Class c, List<FrameData> uframes) throws Exception {
        super(null, file, pointer, size, bd, t, c);

        Map<Integer, UndoChunk> ucs = new HashMap<>();
        for (FrameData uframe : uframes) {
            final DataFrame uf = uframe.getDataFrame();
            for (Chunk udc : uf.getChunks()) {
                UndoChunk uc = (UndoChunk) udc.getEntity();
                final long frameId = file + pointer;
                final long frameId_ = uc.getFile() + uc.getFrame();
                if (frameId == frameId_) {
                    ucs.put(uc.getPtr(), uc);
                }
            }
        }

        int ptr = FRAME_HEADER_SIZE;
        final ByteString bs = new ByteString(this.b);
        while (ptr<this.b.length) {
            if (this.b.length>=ptr+INDEX_HEADER_SIZE) {
                RowHeader h = new RowHeader(bs.substring(ptr, ptr+INDEX_HEADER_SIZE), this.getFile(), this.getPointer());
                if ((h.getPtr()>0)&&(h.getLen()>0)) {
                    final DataChunk dc = new DataChunk(bs.substring(ptr, ptr+INDEX_HEADER_SIZE+h.getLen()), this.getFile(), this.getPointer(), INDEX_HEADER_SIZE, this.getDataObject(), this.getEntityClass());
                    if (this.getType()==INDEX_FRAME_LEAF) {
                        if (INITIALIZE_DURING_CONSTRUCT == 1) {
                            final IndexChunk ib = (IndexChunk) dc.getEntity();
                        }
                    }
                    if (ucs.get(dc.getHeader().getPtr()) != null) {
                        dc.setUndoChunk(ucs.get(dc.getHeader().getPtr()));
                    }
                    data.add(dc);
                    ptr = ptr + INDEX_HEADER_SIZE + h.getLen();
                } else {
                    ptr = this.b.length;
                }
            } else {
                ptr = this.b.length;
            }
        }
        this.b = null; //throw bytes to GC
    }

    //constructor for replication service
    public IndexFrame(byte[] b, int file, long pointer, Map<Long, Long> imap, Map<Long, Long> hmap, Table t) {
        super(b, file, pointer, t);
        int ptr = FRAME_HEADER_SIZE;

        final ByteString bs = new ByteString(this.b);
        while (ptr<this.b.length) {
            if (this.b.length>=ptr+INDEX_HEADER_SIZE) {
                RowHeader h = new RowHeader(bs.substring(ptr, ptr+INDEX_HEADER_SIZE), this.getFile(), this.getPointer());
                if ((h.getPtr()>0)&&(h.getLen()>0)) {
                    //replace framepointers
                    if (h.getFramePtr() > 0) { //IOT does not contains frameptr
                        final long allocId = imap.get(h.getFramePtr());
                        final long bptr = hmap.get(allocId) != null ? hmap.get(allocId) : Instance.getInstance().getFrameByAllocId(allocId).getFrameId();
                        h.getFramePtrRowId().setFileId((int) bptr % 4096);
                        h.getFramePtrRowId().setFramePointer(bptr - (bptr % 4096));
                    }
                    final DataChunk dc = new DataChunk(bs.substring(ptr, ptr+INDEX_HEADER_SIZE+h.getLen()), this.getFile(), this.getPointer(), INDEX_HEADER_SIZE, this.getDataObject(), this.getEntityClass());
                    dc.setHeader(h);
                    data.add(dc);
                    ptr = ptr + INDEX_HEADER_SIZE + h.getLen();
                } else {
                    ptr = this.b.length;
                }
            } else {
                ptr = this.b.length;
            }
        }
        this.b = null; //throw bytes to GC
    }

    @Override
    public synchronized void rollbackTransaction(Transaction tran, ArrayList<FrameData> ubs, Session s) throws Exception {
        data.check();
        final LLT llt = LLT.getLLT();
        llt.add(this);
        final Map<Integer, DataChunk> ucmap = new HashMap();

        if (ubs!=null) {
            for (FrameData ub : ubs) {
                for (Chunk c : ub.getDataFrame().getChunks()) {
                    final UndoChunk uc = (UndoChunk) c.getEntity();
                    final int ucfile = uc.getDataChunk().getHeader().getRowID().getFileId();
                    final long frameptr = uc.getDataChunk().getHeader().getRowID().getFramePointer();
                    if (uc.getTransId() == tran.getTransId() && ucfile == this.getFile() && frameptr == this.getPointer()) {
                        ucmap.put(uc.getPtr(), uc.getDataChunk());
                    }
                }
            }
        }

        //rollback modified index records
        final ArrayList<Integer> r = new ArrayList<>();
        for (Chunk c : data.getChunks()) {
            if (c.getHeader().getTran().getTransId() == tran.getTransId()) {
                final DataChunk dc = ucmap.get(c.getHeader().getPtr());
                if (dc != null) {
                    final DataChunk dc_ = ((IndexChunk) c.getEntity()).getDataChunk().getUndoChunk().getDataChunk();
                    ((DataChunk) c).setUndoChunk(null);
                    ((DataChunk) c).cleanUpIcs();
                    ((IndexChunk) c.getEntity()).setDataChunk(dc_);
                    ((IndexChunk) c.getEntity()).setFramePtrRowId(dc_.getHeader().getRowID());
                    ((DataChunk) c).getHeader().setFramePtr(dc_.getHeader().getRowID());
                    c.getHeader().setState(Header.RECORD_NORMAL_STATE);
                } else {
                    if (c.getHeader().getState() == Header.RECORD_NORMAL_STATE) {
                        r.add(c.getHeader().getPtr());
                    }
                }
            }
        }
        for (Integer i : r) {
            data.removeByPtr(i);
        }

        llt.commit();
    }

    public void cleanICEntities() {
        for (Chunk c : this.data.getChunks()) {
            final Object e = ((DataChunk) c).getExistingEntity();
            if (e != null) {
                ((IndexChunk) e).setDataChunk(null);
            }
        }
    }

    public synchronized FrameData add (DataChunk e, Table t, Session s, LLT llt) throws Exception {
        if (this.isFill(e)) {

            final int nfileId = t.getIndexFileId(this.getFrameData());
            final FrameData res_ = t.createNewFrame(this.getFrameData(), null, nfileId, this.getType(), 0, false, false, false, s, llt);
            final IndexFrame res = res_.getIndexFrame();
            //paranoid fix
            llt.add(res);
            llt.add(this);
            res.setParentF(this.getParentF());
            res.setParentB(this.getParentB());
            final ValueSet max = this.sort();
            if (this.getHasMV()==0) {
                if (e.getDcs().compareTo(max)>0) {
                    res.insertChunk(e, s, false, llt);
                    this.setHasMV(1);
                } else {
                    int inl = e.getBytesAmount();
                    this.insertChunk(e, s, false, llt);
                    this.sort();
                    while (inl>0) {
                        inl = inl - this.data.get(this.data.size()-1).getBytesAmount();
                        res.insertChunk(this.data.get(this.data.size()-1), s, false, true, llt);
                        this.data.remove(this.data.size()-1);
                    }
                    this.setHasMV(1);
                }
                res.setLcId(this.getLcId());
                this.setLcId(0);
            } else {
                //final int cmv = e.getDcs().compareTo(this.getFrameData().getMv());
                final int cmv = e.getDcs().compareTo(max);
                if (cmv > 0) {
                    throw new InternalException();
                } else {
                    res.setDivided(1);
                    this.insertChunk(e, s, false, llt);
                    final ValueSet max2 = this.sort();
                    final ArrayList<DataChunk> nlist = new ArrayList<>();
                    ValueSet pkey = null;
                    boolean keyrpt = false;
                    boolean norpt  = false;

                    int resamt = Frame.FRAME_HEADER_SIZE;
                    for (int i=0; i<this.data.size(); i++) {
                        if (!norpt) {
                            if (pkey!=null) {
                                if (this.data.get(i).getDcs().compareTo(pkey)==0) {
                                    keyrpt = true;
                                }
                            } else {
                                keyrpt = true;
                            }
                        }
                        if (i==this.data.size()-1) {
                            keyrpt = false;
                        }
                        final int half = this.data.size() / 2;
                        if (this.getFrameSize()-resamt>this.data.get(i).getBytesAmount()&&(keyrpt||(this.data.get(i).getDcs().compareTo(max2)<0&&i<=half))) {
                            res.insertChunk(this.data.get(i), s, false, true, llt);
                            resamt = resamt + this.data.get(i).getBytesAmount();
                            res.setHasMV(1);
                        } else {
                            norpt = true;
                            nlist.add((DataChunk)this.data.get(i));
                        }
                        pkey = this.data.get(i).getDcs();
                        keyrpt = false;
                    }
                    this.data.clear();
                    for (DataChunk c : nlist) {
                        this.insertChunk(c, s, false, true, llt);
                    }
                }
            }
            return res_;
        } else {
            this.insertChunk(e, s, false, llt);
            return null;
        }
    }

    public DataChunk get (int index) {
        return (DataChunk)this.data.get(index);
    }

    private boolean isFill(DataChunk ie) {
        return ie.getBytesAmount() > this.getFrameFree();
    }

    public synchronized ValueSet sort() throws InternalException {
        if (!this.data.isSorted()) {
            this.data.sort();
        }
        this.sorted = true;
        if (this.data.size()>0) {
            return ((DataChunk)this.data.get(this.data.size()-1)).getDcs();
        } else {
            return null;
        }
    }

    //accepted only to node element lists
    //for unique indexes
    public synchronized DataChunk getChildElementPtr(ValueSet value) throws InternalException {
        //todo if (!this.sorted) {
            this.sort();
        //}
        for (Chunk ie : this.data.getChunks()) {
            if (((DataChunk)ie).getDcs().compareTo(value)>=0) {
                return ((DataChunk)ie); //known as ptr for node element
            }
        }
        return null;
    }

    //accepted only to node element lists
    //for non-unique indexes
    public synchronized ArrayList<Long> getChildElementsPtr(ValueSet value) throws InternalException {
        //todo if (!this.sorted) {
            this.sort();
        //}
        ArrayList<Long> r = new ArrayList<Long>();
        for (Chunk ie : this.data.getChunks()) {
            if (((DataChunk)ie).getDcs().compareTo(value) == 0) {
                r.add (((DataChunk)ie).getHeader().getFramePtr()); //known as ptr for node element
            }
            if (((DataChunk)ie).getDcs().compareTo(value) > 0) {
                r.add (((DataChunk)ie).getHeader().getFramePtr()); //known as ptr for node element
                break;
            }
        }
        return r;
    }

    //return first element which found - for unique indexes
    public DataChunk getObjectByKey(ValueSet key, Session s) {
        if (this.data.getByKey(key) == null) {
            return null;
        }
        for (Chunk c : this.data.getByKey(key)) {
            final DataChunk dc = (DataChunk) c;
            final long tr = s.getTransaction().getTransId();
            final long mtran = s.getTransaction().getMTran();
            if (dc.getHeader().getState() == Header.RECORD_NORMAL_STATE) {
                if (dc.getUndoChunk() != null && dc.getHeader().getTran().getCid() == 0) { //updated chunk in live transaction
                    return dc;
                } else {
                    if (dc.getHeader().getTran() == null || s.isStream()) {
                        return dc;
                    } else {
                        if ((tr == dc.getHeader().getTran().getTransId()) || (dc.getHeader().getTran().getCid() > 0 && dc.getHeader().getTran().getCid() <= mtran)) {
                            return dc;
                        }
                    }
                }
            }
            if (dc.getHeader().getState() == Header.RECORD_DELETED_STATE) {
                if (dc.getHeader().getTran() != null) {
                    if ((tr != dc.getHeader().getTran().getTransId()) && (dc.getHeader().getTran().getCid() == 0 || dc.getHeader().getTran().getCid() > mtran)) {
                        return dc;
                    }
                }
            }
        }
        return null;
    }

    //return all element which found - for non-unique indexes
    public synchronized List<DataChunk> getObjectsByKey(ValueSet key, Session s) throws InternalException {
        final List<DataChunk> r = new ArrayList<>();
        final long tr = s.getTransaction().getTransId();
        final long mtran = s.getTransaction().getMTran();
        for (Chunk ie : this.data.getChunks()) {
            if (((DataChunk)ie).getDcs().equals(key)) {
                if (((DataChunk)ie).getHeader().getState() == Header.RECORD_NORMAL_STATE) {
                    if (((DataChunk) ie).getUndoChunk() != null && ((DataChunk) ie).getHeader().getTran().getCid() == 0) { //updated chunk in live transaction
                        r.add((DataChunk) ie);
                    } else {
                        if (((DataChunk) ie).getHeader().getTran() == null || s.isStream()) {
                            r.add((DataChunk) ie);
                        } else {
                            if ((tr == ((DataChunk) ie).getHeader().getTran().getTransId()) || (((DataChunk) ie).getHeader().getTran().getCid() > 0 && ((DataChunk) ie).getHeader().getTran().getCid() <= mtran)) {
                                r.add((DataChunk) ie);
                            }
                        }
                    }
                }
                if (((DataChunk)ie).getHeader().getState() == Header.RECORD_DELETED_STATE) {
                    if (((DataChunk)ie).getHeader().getTran()!=null) {
                        if ((tr != ((DataChunk)ie).getHeader().getTran().getTransId()) && (((DataChunk)ie).getHeader().getTran().getCid() == 0 || ((DataChunk)ie).getHeader().getTran().getCid() > mtran)) {
                            r.add((DataChunk)ie);
                        }
                    }
                }
            }
        }
        return r;
    }

    public synchronized int removeObjects(ValueSet key, Object o) throws InternalException {
        int len = 0;
        ArrayList<Integer> d = new ArrayList<Integer>();
        for (int i=0; i<this.data.size(); i++) {
            DataChunk ie = (DataChunk)this.data.get(i);
            if (ie.getDcs().equals(key)) {
                if (ie.getEntity()==o) {
                    len = len + ie.getBytesAmount();
                    d.add(i);
                }
            }
        }
        for (Integer x : d) { this.data.remove(x); }
        return len;
    }

    public synchronized ValueSet getMaxValue() throws InternalException {
        this.sort();
        return ((DataChunk)this.data.get(this.data.size()-1)).getDcs();
    }

    public HashMap<Long, Long> getAllocateMap() {
        final HashMap<Long, Long> imap = new HashMap<Long, Long>();
        for (Chunk c : data.getChunks()) {
            if (c.getHeader().getFramePtr() > 0) {  //IOT does not contains frameptr
                final long allocId = Instance.getInstance().getFrameById(c.getHeader().getFramePtr()).getAllocId();
                imap.put(c.getHeader().getFramePtr(), allocId);
            }
        }
        return imap;
    }

    public synchronized int getType() {
        return this.getRes01();
    }

    public synchronized void setType(int type) {
        this.setRes01(type);
    }

    public synchronized int getHasMV() {
        return this.getRes02();
    }

    public synchronized void setHasMV(int hasMV) {
        this.setRes02(hasMV);
    }

    public synchronized int getDivided() {
        return this.getRes03();
    }

    public synchronized void setDivided(int divided) {
        this.setRes03(divided);
    }

    public synchronized int getParentF() {
        return this.getRes04();
    }

    public synchronized void setParentF(int parentF) {
        this.setRes04(parentF);
    }

    public synchronized long getParentB() {
        return this.getRes06();
    }

    public synchronized void setParentB(long parentB) {
        this.setRes06(parentB);
    }

    public synchronized long getLcId() {
        return this.getRes05() + this.getRes07();
    }

    public synchronized void setLcId(long lcId) {
        final long lcF = lcId%4096;
        this.setRes05((int)lcF);
        this.setRes07(lcId - lcF);
    }
}
