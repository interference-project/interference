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

package su.interference.persistent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.*;
import su.interference.mgmt.MgmtColumn;
import su.interference.sql.FrameApi;
import su.interference.transport.CommandEvent;
import su.interference.transport.TransportSyncTask;

import javax.persistence.Entity;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Transient;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@Entity
@SystemEntity
@DisableSync
public class FrameData implements Serializable, Comparable, FrameApi, FilePartitioned, OnDelete {

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
    private int frameType;
    @Column
    private int current;
    @Column
    private volatile int started;
    @Column
    private volatile long frameOrder;
    @Id
    @MapColumn
    @Transient
    private long frameId; //virtual Id field
    @Transient
    private final Map<Long, Map<Long, TransFrame>> tcounter = new ConcurrentHashMap<>();
    @Transient
    private AtomicInteger priority = new AtomicInteger(2);
    @Transient
    private AtomicBoolean synced = new AtomicBoolean(true);
    @Transient
    private AtomicInteger lock = new AtomicInteger(0);
    @Transient
    private volatile boolean rbck;
    @Transient
    private volatile Frame frame;
    @Transient
    private AtomicInteger freed = new AtomicInteger(0);
    @Transient
    private Table dataObject;
    @Transient
    private Class entityClass;
    @Transient
    private DataFile dataFile;
    //moved from IndexFrame - non-persistent value (must set if hasMv=1)
    @Transient
    private ValueSet mv;

    @Override
    public int getImpl() {
        return FrameApi.IMPL_DATA;
    }

    public synchronized void markAsCurrent() {
        this.current = 1;
    }

    public synchronized void clearCurrent() {
        this.current = 0;
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
            this.priority.set(SystemCleanUp.DATA_RETRIEVED_PRIORITY);
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

    public synchronized IndexFrame getIndexFrame() throws Exception {
        if (frame == null) {
            this.priority.set(SystemCleanUp.INDEX_RETRIEVED_PRIORITY);
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

    @Override
    public ArrayList<Chunk> getFrameChunks(Session s) throws Exception {
        if (getDataObject().isIndex()) {
            final IndexFrame f = getIndexFrame();
            synchronized (this) {
                f.sort();
            }
            return f.getFrameChunks(s);
        } else {
            return getDataFrame().getFrameChunks(s);
        }
    }

    @Override
    public ArrayList<Object> getFrameEntities(Session s) throws Exception {
        final ArrayList<Object> res = new ArrayList<>();
        for (Chunk c : getFrameChunks(s)) {
            if (isIndex() || isNoTran()) {
                res.add(c.getEntity());
            } else {
                res.add(((EntityContainer) c.getEntity()).getEntity(s));
            }
        }
        return res;
    }

    public Frame getFrame() throws Exception {
        if (frame == null) {
            if (isIndex()) {
                frame = getIndexFrame();
            } else {
                frame = getDataFrame();
            }
            frame.setFrameData(this);
        }
        return frame;
    }

    public synchronized Chunk getChunkByPtr(int ptr) throws Exception {
        return getFrame().getChunkByPtr(ptr);
    }

    public int getFrameType() {
        return frameType;
    }

    public void setFrameType(int frameType) {
        this.frameType = frameType;
    }

    public boolean isIndex() {
        return getDataObject().isIndex();
    }

    public boolean isNoTran() {
        return getDataObject().isNoTran();
    }

    public boolean isFrame() {
        return frame != null;
    }

    public void setFrame(Frame b) {
        this.frame = b;
    }

    @Override
    public long getFrameId() {
        return this.ptr+this.file;
    }

    @Override
    public long getAllocId() {
        return this.allocId;
    }

    public long getAllocFile() {
        return this.allocId%4096;
    }

    public long getAllocPtr() {
        return this.allocId - (this.allocId%4096);
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
    public synchronized int getFrameUsed() {
        int tdiff = 0;
        for (Map.Entry<Long, Map<Long, TransFrame>> entry : tcounter.entrySet()) {
            for (Map.Entry<Long, TransFrame> entry_ : entry.getValue().entrySet()) {
                final int tdiff_ = entry_.getValue().getDiff() > 0 ? entry_.getValue().getDiff() : 0;
                tdiff = tdiff + tdiff_;
            }
        }
        return this.getUsed() + tdiff;
    }

    public int getFrameFree() {
        return size - Frame.FRAME_HEADER_SIZE - getFrameUsed();
    }

    public int getFrameFreeNoTran() {
        return size - Frame.FRAME_HEADER_SIZE - getUsed();
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
        this.current = bd.getCurrent();
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
        return this.getDataFrame().insertChunk(c, s, check, llt);
    }

    public int updateChunk(DataChunk chunk, Object o, Session s, LLT llt) throws Exception {
        return this.getDataFrame().updateChunk(chunk, o, s, llt);
    }

    public void removeChunk(int ptr, Session s, LLT llt) throws Exception {
        this.getFrame().removeChunk(ptr, llt, false);
    }

    public void deleteChunk(int ptr, Session s, LLT llt, boolean ignoreNoLocal) throws Exception {
        this.getFrame().deleteChunk(ptr, s, llt, ignoreNoLocal);
    }

    public DataFile getDataFile() {
        if (this.dataFile == null) {
            this.dataFile = Instance.getInstance().getDataFileById(this.file);
        }
        return this.dataFile;
    }

    @Override
    public int getObjectId() {
        return objectId;
    }

    @Override
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

    public synchronized boolean clearFrame() {
        if (this.frame != null && this.isSynced()) {
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

    public synchronized boolean isFrameBusy() {
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

    public int checkTcounter(long id) {
        Map<Long, TransFrame> map = tcounter.get(id);
        return map == null ? -1 : map.size();
    }

    public synchronized Map<Long, List<Long>> getLiveUFrameAllocIds() {
        final Map<Long, List<Long>> uframes = new HashMap<>();
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

    public synchronized void updateTCounter(long transId, List<Long> ulist) {
        if (this.frameId == this.allocId) {
            throw new RuntimeException("cannot update tcounter for local frame");
        }
        this.tcounter.put(transId, new HashMap<>());
        for (Long l : ulist) {
            this.tcounter.get(transId).put(l, null);
        }
    }

    public synchronized void rollbackTransaction(Transaction tran, ArrayList<FrameData> ubs, Session s) throws Exception {
        this.getFrame().rollbackTransaction(tran, ubs, s);
    }

    public int getOwnerId() {
        final long ownerId = this.getAllocFile()/Storage.MAX_NODES + 1;
        return (int)ownerId;
    }

    public int getPriority() {
        return priority.get();
    }

    public void decreasePriority() {
        if (this.priority.get() > 0) {
            this.priority.decrementAndGet();
        }
    }

/*
    public int getLock() {
        return lock.get();
    }
*/

    public boolean isLocal() {
        return this.getFrameId() == this.getAllocId();
    }

    public boolean isLockedLocally() {
        return this.lock.get() == Config.getConfig().LOCAL_NODE_ID;
    }

    public synchronized boolean lock(long transId) throws Exception {
        if (this.lock.compareAndSet(Config.getConfig().LOCAL_NODE_ID, Config.getConfig().LOCAL_NODE_ID)) {
            return true;
        }
        if (this.lock.compareAndSet(0, Config.getConfig().LOCAL_NODE_ID)) {
            if (this.isLocal()) {
                return true;
            } else {
                final boolean success = TransportSyncTask.sendNoPersistCommand(this.getOwnerId(), CommandEvent.LOCK_FRAME, transId, this.getAllocId());
                if (!success) {
                    this.lock.compareAndSet(Config.getConfig().LOCAL_NODE_ID, 0);
                    return false;
                }
                return true;
            }
        }
        return false;
    }

    public synchronized boolean rlock(long transId, int nodeId) {
        if (this.isLocal()) {
            if (this.lock.compareAndSet(0, nodeId)) {
                return true;
            }
            if (this.lock.compareAndSet(nodeId, nodeId)) {
                return true;
            }
        } else {
            throw new RuntimeException("rlock method is not applicable to non-local frames");
        }
        return false;
    }

/*
    public void unlock(int lock) {
        this.lock.compareAndSet(lock, 0);
    }
*/

    public boolean isSynced() {
        return synced.get();
    }

    public void setSynced() {
        this.synced.set(true);
    }

    public void setUnsynced() {
        this.synced.set(false);
    }

    public boolean isRbck() {
        return rbck;
    }

    public void setRbck(boolean rbck) {
        this.rbck = rbck;
    }

    public synchronized int getCurrent() {
        return current;
    }

    public int getStarted() {
        return started;
    }

    public void setStarted(int started) {
        this.started = started;
    }

    @Override
    public long getFrameOrder() {
        return frameOrder;
    }

    public void setFrameOrder(long frameOrder) {
        this.frameOrder = frameOrder;
    }

    public synchronized ValueSet getMv() {
        return mv;
    }

    public synchronized void setMv(ValueSet mv) {
        this.mv = mv;
    }

    public boolean isFree() {
        return freed.compareAndSet(1, 1);
    }

    public boolean isValid() {
        return freed.compareAndSet(0, 0);
    }

    public void free() {
        this.freed.compareAndSet(0, 1);
    }

    public void onDelete() {
        this.frame = null;
        this.free();
    }

    @Override
    public boolean isProcess() {
        return false;
    }

    @Override
    public Class getEventProcessor() {
        return null;
    }
}
