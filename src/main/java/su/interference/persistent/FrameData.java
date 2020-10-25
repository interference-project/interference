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
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
    private int distribution;
    @Column
    private AtomicInteger current;
    @Column
    private volatile int started;
    @Column
    private volatile long frameOrder;
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
    private final Map<Long, Map<Long, TransFrame>> tcounter = new ConcurrentHashMap<>();
    @Transient
    private volatile int priority = 2;
    @Transient
    private volatile boolean synced = true;
    @Transient
    private volatile Frame frame;
    @Transient
    private Table dataObject;
    @Transient
    private Class entityClass;
    @Transient
    private DataFile dataFile;

    public int getImpl() {
        return FrameApi.IMPL_DATA;
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

    public Table getDataObject() {
        if (dataObject==null) {
            dataObject = Instance.getInstance().getTableById(this.objectId);
        }
        return dataObject;
    }

    public void setDataObject(Table dataObject) {
        this.dataObject = dataObject;
    }

    public synchronized DataFrame getDataFrame() throws Exception {
        if (frame == null) {
            this.priority = SystemCleanUp.DATA_RETRIEVED_PRIORITY;
            List<FrameData> uframes = new ArrayList<>();
            for (Map.Entry<Long, Map<Long, TransFrame>> entry : tcounter.entrySet()) {
                for (Map.Entry<Long, TransFrame> entry_ : entry.getValue().entrySet()) {
                    if (entry_.getKey() > 0) {
                        uframes.add(Instance.getInstance().getFrameById(entry_.getKey()));
                    }
                }
            }
            frame = new DataFrame(this.file, this.ptr, 0, this, dataObject, entityClass, uframes);
        }
        return (DataFrame) frame;
    }

    public IndexFrame getIndexFrame() throws Exception {
        if (frame == null) {
            this.priority = SystemCleanUp.INDEX_RETRIEVED_PRIORITY;
            List<FrameData> uframes = new ArrayList<>();
            for (Map.Entry<Long, Map<Long, TransFrame>> entry : tcounter.entrySet()) {
                for (Map.Entry<Long, TransFrame> entry_ : entry.getValue().entrySet()) {
                    if (entry_.getKey() > 0) {
                        uframes.add(Instance.getInstance().getFrameById(entry_.getKey()));
                    }
                }
            }
            frame = new IndexFrame(this.file, this.ptr, 0, this, dataObject, entityClass, uframes);
        }
        return (IndexFrame) frame;
    }

    public synchronized ArrayList<Chunk> getFrameChunks(Session s) throws Exception {
        if (getDataObject().isIndex()) {
            return getIndexFrame().getFrameChunks(s);
        } else {
            return getDataFrame().getFrameChunks(s);
        }
    }

    public synchronized ArrayList<Object> getFrameEntities(Session s) throws Exception {
        final ArrayList<Object> res = new ArrayList<>();
        for (Chunk c : getFrameChunks(s)) {
            res.add(((DataChunk)c).getEntity());
        }
        return res;
    }

    public Frame getFrame() throws Exception {
        if (frame == null) {
            if (getDataObject().isIndex()) {
                frame = getIndexFrame();
            } else {
                frame = getDataFrame();
            }
        }
        return frame;
    }

    public boolean isFrame() {
        return frame != null;
    }

    public void setFrame(Frame b) {
        this.frame = b;
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

    //real current transactional value
    //skip negative differences for prevent frame oversize when many transactions change data
    public int getFrameUsed() {
        int tdiff = 0;
        for (Map.Entry<Long, Map<Long, TransFrame>> entry : tcounter.entrySet()) {
            for (Map.Entry<Long, TransFrame> entry_ : entry.getValue().entrySet()) {
                final int tdiff_ = entry_.getValue().getDiff() > 0 ? entry_.getValue().getDiff() : 0;
                tdiff = tdiff + tdiff_;
            }
        }
        return this.getUsed() + tdiff;
    }

    //real current non-tran amount of chunk bytes
    public int getCUsed() {
        if (frame ==null) {
            return -1;
        }
        return frame.getBytesAmount();
    }

    public int getFrameFree() throws InternalException {
        return size - Frame.FRAME_HEADER_SIZE - getFrameUsed();
    }

    public FrameData() {

    }

    public FrameData(int file, long ptr, int size, Table t) {
        this.dataObject = t;
        this.objectId = t.getObjectId();
        this.allocId = file + ptr;
        this.file = file;
        this.ptr  = ptr;
        this.size = size;
    }

    public FrameData(FrameData bd, Table t) {
        this.dataObject = t;
        this.objectId = t.getObjectId();
        this.allocId = bd.getFile() + bd.getPtr();
        this.file = bd.getFile();
        this.dataFile = Instance.getInstance().getDataFileById(this.file);
        this.ptr  = bd.getPtr();
        this.size = bd.getSize();
        this.allocId = bd.getAllocId();
        this.started = bd.getStarted();
        this.current = new AtomicInteger(bd.getCurrent().get());
        this.used = bd.getUsed();
        this.prevFrame = bd.getPrevFrame();
        this.prevFile = bd.getPrevFile();
        this.nextFrame = bd.getNextFrame();
        this.nextFile = bd.getNextFile();
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

    public int insertChunk(Chunk c, Session s, boolean check, LLT llt) throws Exception {
        int p = this.getDataFrame().insertChunk(c, s, check, llt);
        return p;
    }

    public int updateChunk(DataChunk chunk, Object o, Session s, LLT llt) throws Exception {
        return this.getDataFrame().updateChunk(chunk, o, s, llt);
    }

    public void removeChunk(int ptr, Session s, LLT llt) throws Exception {
        this.getFrame().removeChunk(ptr, llt, false);
    }

    public void deleteChunk(int ptr, Session s, LLT llt) throws Exception {
        this.getFrame().deleteChunk(ptr, s, llt);
    }

    public DataFile getDataFile() {
        if (this.dataFile == null) {
            this.dataFile = Instance.getInstance().getDataFileById(this.file);
        }
        return this.dataFile;
    }

    public int getObjectId() {
        return objectId;
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

    public boolean clearFrame() {
        if (this.frame != null) {
            this.frame.cleanUpIcs();
            this.frame = null;
            return true;
        }
        return false;
    }

    public Class getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(Class entityClass) {
        this.entityClass = entityClass;
    }

    public boolean isFrameBusy() {
        for (Map.Entry<Long, Map<Long, TransFrame>> entry : tcounter.entrySet()) {
            for (Map.Entry<Long, TransFrame> entry_ : entry.getValue().entrySet()) {
                if (entry_.getValue().getCframeId() == getFrameId()) {
                    return true;
                }
            }
        }
        return false;
    }

    public void increaseTcounter(long id, TransFrame f) {
        if (tcounter.get(id) == null) {
            tcounter.put(id, new ConcurrentHashMap<>());
        }
        tcounter.get(id).put(f.getUframeId(), f);
    }

    public void decreaseTcounter(long id) {
        tcounter.remove(id);
    }

    public Map<Long, List<Long>> getLiveUFrameAllocIds() {
        Map<Long, List<Long>> uframes = new HashMap<>();
        for (Map.Entry<Long, Map<Long, TransFrame>> entry : tcounter.entrySet()) {
            uframes.put(entry.getKey(), new ArrayList<>());
            for (Map.Entry<Long, TransFrame> entry_ : entry.getValue().entrySet()) {
                if (entry_.getValue().getUframeId() > 0) {
                    final FrameData uframe = Instance.getInstance().getFrameById(entry_.getValue().getUframeId());
                    // may causes NPE during rollback
                    if (uframe != null) {
                        uframes.get(entry.getKey()).add(uframe.getAllocId());
                    }
                }
            }
        }
        return uframes;
    }

    public void updateTCounter(long transId, List<Long> ulist) {
        if (this.frameId == this.allocId) {
            throw new RuntimeException("cannot update tcounter for local frame");
        }
        this.tcounter.put(transId, new HashMap<>());
        for (Long l : ulist) {
            this.tcounter.get(transId).put(l, null);
        }
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public void decreasePriority() {
        if (this.priority > 0) {
            this.priority--;
        }
    }

    public boolean isSynced() {
        return synced;
    }

    public void setSynced(boolean synced) {
        this.synced = synced;
    }

    public int getDistribution() {
        return distribution;
    }

    public void setDistribution(int distribution) {
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

    public long getFrameOrder() {
        return frameOrder;
    }

    public void setFrameOrder(long frameOrder) {
        this.frameOrder = frameOrder;
    }
}
