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

import su.interference.persistent.FrameData;
import su.interference.persistent.Table;
import su.interference.persistent.Session;
import su.interference.persistent.UndoChunk;
import su.interference.exception.*;
import su.interference.serialize.ByteString;

import java.util.*;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class DataFrame extends Frame {

    public DataFrame(int file, long pointer, int size, Table t) throws InternalException {
        super (file, pointer, size, t);
    }

    public DataFrame(FrameData bd, Table t) throws InternalException {
        super (bd, t);
    }

    public DataFrame(int file, long pointer, int size, FrameData bd, Table t, Class c) throws Exception {
        super(null, file, pointer, size, bd, t, c);

        int ptr = FRAME_HEADER_SIZE;
        final ByteString bs = new ByteString(this.b);
        while (ptr<this.b.length) {
            if (this.b.length>=ptr+ROW_HEADER_SIZE) {
                final RowHeader h = new RowHeader(bs.substring(ptr, ptr+ROW_HEADER_SIZE), this.getFile(), this.getPointer());
                if ((h.getPtr()>0)&&(h.getLen()>0)) {
                    data.add(new DataChunk(bs.substring(ptr, ptr+ROW_HEADER_SIZE+h.getLen()), this.getFile(), this.getPointer(), ROW_HEADER_SIZE, this.getDataObject(), this.getEntityClass()));
                    ptr = ptr + ROW_HEADER_SIZE + h.getLen();
                } else {
                    ptr = this.b.length;
                }
            } else {
                ptr = this.b.length;
            }
        }
        this.b = null; //throw bytes to GC
    }

    //constructor for replication service - data frames
    public DataFrame(byte[] b, int file, long pointer, Map<Long, List<Chunk>> umap, Table t) throws Exception {
        super(b, file, pointer, t);

        int ptr = FRAME_HEADER_SIZE;
        final Map<Integer, UndoChunk> ucmap = new HashMap<>();
        if (umap.get(file + pointer) != null) {
            for (Chunk c : umap.get(file + pointer)) {
                ucmap.put(((UndoChunk) c.getEntity()).getPtr(), (UndoChunk) c.getEntity());
            }
        }
        final ByteString bs = new ByteString(this.b);
        while (ptr<this.b.length) {
            if (this.b.length>=ptr+ROW_HEADER_SIZE) {
                final RowHeader h = new RowHeader(bs.substring(ptr, ptr+ROW_HEADER_SIZE), this.getFile(), this.getPointer());
                if ((h.getPtr()>0)&&(h.getLen()>0)) {
                    final DataChunk dc = new DataChunk(bs.substring(ptr, ptr+ROW_HEADER_SIZE+h.getLen()), this.getFile(), this.getPointer(), ROW_HEADER_SIZE, this.getDataObject(), this.getEntityClass());
                    dc.setUndoChunk(ucmap.get(h.getPtr()));
                    dc.getHeader().setTran(dc.getHeader().getTran());
                    data.add(dc);
                    ptr = ptr + ROW_HEADER_SIZE + h.getLen();
                } else {
                    ptr = this.b.length;
                }
            } else {
                ptr = this.b.length;
            }
        }
        this.b = null; //throw bytes to GC
    }

    // constructor for replication service - undo frames
    public DataFrame(byte[] b, int file, long pointer, HashMap<Long, Long> imap, HashMap<Long, Long> hmap, Map<Long, List<Chunk>> umap, Table t, Session s) throws Exception {
        super(b, file, pointer, t);

        if (!t.getName().equals("su.interference.persistent.UndoChunk")) {
            throw new InternalException();
        }

        int ptr = FRAME_HEADER_SIZE;
        final ByteString bs = new ByteString(this.b);
        while (ptr<this.b.length) {
            if (this.b.length>=ptr+ROW_HEADER_SIZE) {
                final RowHeader h = new RowHeader(bs.substring(ptr, ptr+ROW_HEADER_SIZE), this.getFile(), this.getPointer());
                if ((h.getPtr()>0)&&(h.getLen()>0)) {
                    //replace framepointers
                    final DataChunk dc = new DataChunk(bs.substring(ptr, ptr+ROW_HEADER_SIZE+h.getLen()), this.getFile(), this.getPointer(), ROW_HEADER_SIZE, this.getDataObject(), this.getEntityClass());
                    final UndoChunk uc = (UndoChunk)dc.getEntity();
                    final long allocId = imap.get(uc.getFile() + uc.getFrame());
                    final long bptr = hmap.get(allocId) != null ? hmap.get(allocId) : Instance.getInstance().getFrameByAllocId(allocId).getFrameId();
                    uc.setFile((int) bptr % 4096);
                    uc.setFrame(bptr - (bptr % 4096));
                    data.add(dc);
                    if (umap.get(bptr) == null) {
                        umap.put(bptr, new ArrayList<>());
                    }
                    umap.get(bptr).add(dc);
                    ptr = ptr + ROW_HEADER_SIZE + h.getLen();
                } else {
                    ptr = this.b.length;
                }
            } else {
                ptr = this.b.length;
            }
        }
        this.b = null; //throw bytes to GC
    }

    // allocate map for undo frames
    public HashMap<Long, Long> getAllocateMap() throws Exception {
        if (getDataObject().getName().equals("su.interference.persistent.UndoChunk")) {
            final HashMap<Long, Long> imap = new HashMap<Long, Long>();
            for (Chunk c : data.getChunks()) {
                final UndoChunk uc = (UndoChunk) c.getEntity();
                final long allocId = Instance.getInstance().getFrameById(uc.getFile() + uc.getFrame()).getAllocId();
                imap.put(uc.getFile() + uc.getFrame(), allocId);
            }
            return imap;
        }
        return null;
    }

    public int getPrevFile() {
        return this.getRes01();
    }

    public void setPrevFile(int prevFile) {
        this.setRes01(prevFile);
    }

    public long getPrevFrame() {
        return this.getRes06();
    }

    public void setPrevFrame(long prevFrame) {
        this.setRes06(prevFrame);
    }

    public int getNextFile() {
        return this.getRes02();
    }

    public void setNextFile(int nextFile) {
        this.setRes02(nextFile);
    }

    public long getNextFrame() {
        return this.getRes07();
    }

    public void setNextFrame(long nextFrame) {
        this.setRes07(nextFrame);
    }

}
