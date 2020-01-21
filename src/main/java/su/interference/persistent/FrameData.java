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

package su.interference.persistent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.*;
import su.interference.mgmt.MgmtColumn;
import su.interference.exception.InternalException;
import su.interference.sql.FrameApi;

import javax.persistence.Entity;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Transient;
import java.lang.reflect.InvocationTargetException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
@DisableSync
public class FrameData implements Serializable, Comparable, FrameApi, FilePartitioned {

    @Transient
    private final static long serialVersionUID = 8712349857239487288L;
    @Transient
    private final static Logger logger = LoggerFactory.getLogger(FrameData.class);
    @Transient
    public static final int CLASS_ID = 3;

    @Column
    @IndexColumn
    @MgmtColumn(width=20, show=true, form=false, edit=false)
    private volatile int objectId;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private volatile int file;
    @Column
    @MgmtColumn(width=30, show=true, form=true, edit=false)
    private volatile long ptr;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private volatile int size;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private volatile int used;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private volatile int prevFile;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private volatile long prevFrame;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private volatile int nextFile;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private volatile long nextFrame;
    @Column
    @MapColumn
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private volatile long allocId; //virtual Id field
    @Column
    private AtomicLong distribution;
    @Column
    private AtomicInteger current;
    @Column
    @IndexColumn
    private volatile int started;
    @Column
    @IndexColumn
    private AtomicInteger allocated;
    @Id
    @MapColumn
    @Transient
    private long frameId; //virtual Id field
    @Transient
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int frameUsed;
    @Transient
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int cUsed;
    @Transient
    private int undoId;  //for UndoChunk: objectId
    @Transient
    private long transId; //for UndoChunk: transId
    @Transient
    private volatile Frame frame;
    @Transient
    private DataObject dataObject;
    @Transient
    private Class entityClass;
    @Transient
    private volatile WaitFrame waitFrame;

    public int getImpl() {
        return FrameApi.IMPL_DATA;
    }

    public void release() throws InternalException {
        if (current == null || current.get() == 0) {
            return;
        }
        if (waitFrame==null) {
            throw new InternalException();
        } else {
            waitFrame.release();
        }
    }

    public void setWaitFrame(WaitFrame waitFrame) {
        this.waitFrame = waitFrame;
    }

    public void markAsCurrent() {
        if (this.current==null) {
            this.current = new AtomicInteger(0);
        }
        this.current.compareAndSet(0, 1);
    }

    public void clearCurrent() {
        if (this.current!=null) {
            this.current.compareAndSet(1, 0);
        }
    }

    public void allocate() {
        if (this.allocated==null) {
            this.allocated = new AtomicInteger(0);
        }
        this.allocated.compareAndSet(0, 1);
    }

    public void deallocate() {
        if (this.allocated!=null) {
            this.allocated.compareAndSet(1, 0);
        }
    }

    public DataObject getDataObject() {
        if (dataObject==null) {
            dataObject = Instance.getInstance().getTableById(this.objectId);
        }
        return dataObject;
    }

    public void setDataObject(DataObject dataObject) {
        this.dataObject = dataObject;
    }

    public synchronized DataFrame getDataFrame() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException {
        if (frame ==null) {
            frame = new DataFrame(this.file,this.ptr,0,this,dataObject,entityClass);
        }
        return (DataFrame) frame;
    }

    public IndexFrame getIndexFrame() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException {
        if (frame ==null) {
            frame = new IndexFrame(this.file,this.ptr,0,this,dataObject,entityClass);
        }
        return (IndexFrame) frame;
    }

    public synchronized ArrayList<Chunk> getFrameChunks(Session s)
            throws IOException, ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        if (getDataObject().isIndex()) {
            return getIndexFrame().getFrameChunks(s);
        } else {
            return getDataFrame().getFrameChunks(s);
        }
    }

    public synchronized ArrayList<Object> getFrameEntities(Session s)
        throws IOException, ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        final ArrayList<Object> res = new ArrayList<>();
        for (Chunk c : getFrameChunks(s)) {
            res.add(((DataChunk)c).getEntity());
        }
        return res;
    }

    public Frame getFrame() throws IOException, ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        if (frame ==null) {
            if (getDataObject().isIndex()) {
                frame = getIndexFrame();
            } else {
                frame = getDataFrame();
            }
        }
        return frame;
    }

    public void setFrame(Frame b) {
        this.frame = b;
    }

    public FrameData() {
        this.allocated = new AtomicInteger();
    }

    public long getFrameId() {
        return this.ptr+this.file;
    }

    public long getAllocId() {
        return this.allocId;
    }

    public void setAllocId(long allocId) {
        this.allocId = allocId;
    }

    public long getPrevFrameId() {
        return this.prevFile+this.prevFrame;
    }

    public long getNextFrameId() {
        return this.nextFile+this.nextFrame;
    }

    protected int getFrameSize() throws ClassNotFoundException, InternalException, IllegalAccessException, InstantiationException {
        return Instance.getInstance().getFrameSize();
    }

    //real current transactional value
    //skip negative differences for prevent frame oversize when many transactions change data
    public int getFrameUsed() {
        final ArrayList<TransFrame> tbs = Instance.getInstance().getTransFrames(this.getFrameId());
        int tdiff = 0;
        for (TransFrame tb : tbs) {
            tdiff = tdiff + tb.getDiff();
        }
        return this.getUsed() + (tdiff>0?tdiff:0);
    }

    //real current non-tran amount of chunk bytes
    public int getCUsed() {
        if (frame ==null) {
            return -1;
        }
        return frame.getBytesAmount();
    }

    public int getFrameFree() throws ClassNotFoundException, InternalException, InstantiationException, IllegalAccessException {
        return getFrameSize() - Frame.FRAME_HEADER_SIZE - getFrameUsed();
    }

    public boolean hasLiveTransaction(long transId) {
        //Map<Long, Transaction> tmap = frame.getLiveTransactions();
        final ArrayList<TransFrame> tbs = Instance.getInstance().getTransFrames(this.getFrameId());
        for (TransFrame tb : tbs) {
            if (tb.getTransId() == transId) {
                return true;
            }
        }
        return false;
    }

    public boolean hasLocalTransactions() {
        final ArrayList<TransFrame> tbs = Instance.getInstance().getTransFrames(this.getFrameId());
        for (TransFrame tb : tbs) {
            final Transaction tran = Instance.getInstance().getTransactionById(tb.getTransId());
            if (tran.getNodeId() == Config.getConfig().LOCAL_NODE_ID) {
                return true;
            }
        }
        return false;
    }

    //returns node id
    public int hasRemoteTransactions() throws InternalException {
        final ArrayList<TransFrame> tbs = Instance.getInstance().getTransFrames(this.getFrameId());
        boolean local = false;
        int nodeId = 0;
        for (TransFrame tb : tbs) {
            final Transaction tran = Instance.getInstance().getTransactionById(tb.getTransId());
            if (tran.getNodeId() == Config.getConfig().LOCAL_NODE_ID) {
                local = true;
            } else {
                if (nodeId > 0 && nodeId != tran.getNodeId()) {
                    throw new InternalException();
                }
                nodeId = tran.getNodeId();
            }
            if (local == true && nodeId > 0) {
                throw new InternalException();
            }
        }
        return nodeId;
    }

    public FrameData(int file, long ptr, int size, DataObject tt) {
        this.dataObject = tt;
        this.objectId = tt.getObjectId();
        this.allocId = file+ptr;
        this.file = file;
        this.ptr  = ptr;
        this.size = size;
        this.allocated = new AtomicInteger();
    }

    public int compareTo(Object obj) {
        final FrameData c = (FrameData)obj;
        if (this.getFile() == c.getFile()) {
            if (this.getPtr() < c.getPtr()) {
                return -1;
            } else if (this.getPtr() > c.getPtr()) {
                return 1;
            }
        } else {
            if (this.getFile() < c.getFile()) {
                return -1;
            } else if (this.getFile() > c.getFile()) {
                return 1;
            }
        }
        return 0;
    }

    public int insertChunk(Chunk c, Session s, boolean check, LLT llt) throws ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, IOException, NoSuchMethodException, InvocationTargetException {
        int p = this.getDataFrame().insertChunk(c, s, check, llt);
        return p;
    }

    public int updateChunk(DataChunk chunk, Object o, Session s, LLT llt) throws ClassNotFoundException, InvocationTargetException, IOException, NoSuchMethodException, InternalException, IllegalAccessException, InstantiationException {
        return this.getDataFrame().updateChunk(chunk, o, s, llt);
    }

    public void removeChunk(int ptr, Session s, LLT llt) throws ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, IOException {
        this.getDataFrame().removeChunk(ptr, llt, false);
    }

    public void deleteChunk(int ptr, Session s, LLT llt) throws ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, IOException, NoSuchMethodException, InvocationTargetException {
        this.getDataFrame().deleteChunk(ptr, s, llt);
    }

    public int getObjectId() {
        return objectId;
    }

    public void setObjectId(int objectId) {
        this.objectId = objectId;
    }

    public int getFile() {
        return file;
    }

    public void setFile(int file) {
        this.file = file;
    }

    public long getPtr() {
        return ptr;
    }

    public void setPtr(long ptr) {
        this.ptr = ptr;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public synchronized int getUsed() {
        return used;
    }

    public synchronized void setUsed(int used) {
        this.used = used;
    }

    public int getPrevFile() {
        return prevFile;
    }

    public long getPrevFrame() {
        return prevFrame;
    }

    public int getNextFile() {
        return nextFile;
    }

    public long getNextFrame() {
        return nextFrame;
    }

    public void setPrevFile(int prevFile) {
        this.prevFile = prevFile;
        if (this.frame !=null) {
            if (this.frame.getClass().getName().equals("su.interference.core.DataFrame")) {
                ((DataFrame)this.frame).setPrevFile(prevFile);
            }
        }
    }

    public void setPrevFrame(long prevFrame) {
        this.prevFrame = prevFrame;
        if (this.frame !=null) {
            if (this.frame.getClass().getName().equals("su.interference.core.DataFrame")) {
                ((DataFrame)this.frame).setPrevFrame(prevFrame);
            }
        }
    }

    public void setNextFile(int nextFile) {
        this.nextFile = nextFile;
        if (this.frame !=null) {
            if (this.frame.getClass().getName().equals("su.interference.core.DataFrame")) {
                ((DataFrame)this.frame).setNextFile(nextFile);
            }
        }
    }

    public void setNextFrame(long nextFrame) {
        this.nextFrame = nextFrame;
        if (this.frame !=null) {
            if (this.frame.getClass().getName().equals("su.interference.core.DataFrame")) {
                ((DataFrame)this.frame).setNextFrame(nextFrame);
            }    
        }
    }

    public Class getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(Class entityClass) {
        this.entityClass = entityClass;
    }

    public AtomicLong getDistribution() {
        return distribution;
    }

    public void setDistribution(AtomicLong distribution) {
        this.distribution = distribution;
    }

    public AtomicInteger getCurrent() {
        return current;
    }

    public void setCurrent(AtomicInteger current) {
        this.current = current;
    }

    public int getStarted() {
        return started;
    }

    public void setStarted(int started) {
        this.started = started;
    }

    public AtomicInteger getAllocated() {
        return allocated;
    }

    public void setAllocated(AtomicInteger allocated) {
        this.allocated = allocated;
    }
}
