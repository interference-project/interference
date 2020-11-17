/**
 The MIT License (MIT)

 Copyright (c) 2010-2020 head systems, ltd

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
import su.interference.exception.InternalException;
import su.interference.exception.CannotAccessToLockedRecord;
import su.interference.persistent.*;
import su.interference.persistent.Table;
import su.interference.serialize.ByteString;
import su.interference.serialize.CustomSerializer;

import javax.persistence.*;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.lang.reflect.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static su.interference.persistent.Table.SYSTEM_PKG_PREFIX;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@SuppressWarnings("unchecked")
public class DataChunk implements Chunk {

    private final static Logger logger = LoggerFactory.getLogger(DataChunk.class);
    private final static int INIT_STATE = 1;
    private final static int NORMAL_STATE = 2;
    private Table t;
    private Class class_;
    private volatile int state;
    private volatile RowHeader header;
    private volatile byte[] chunk;
    private Comparable id;
    private byte[] serializedId;
    private Object entity;
    private Object undoentity;
    private Map<Integer, DataChunk> ics = new HashMap<>();
    private UndoChunk uc;
    private FrameData uframe;
    private boolean terminate;
    private final CustomSerializer sr = new CustomSerializer();

    //returns datacolumn set
    public ValueSet getDcs() {
        if (state == INIT_STATE) {
            return getDcsFromBytes();
        }
        if (state == NORMAL_STATE) {
            return getDcsFromEntity();
        }
        return null;
    }

    //returns datacolumn set
    private ValueSet getDcsFromEntity() {
        ValueSet dcs = null;
        try {
            final Field[] f = this.t == null ? this.entity.getClass().getDeclaredFields() : t.getFields();
            final List<Object> vs = new ArrayList<>();
            for (int i = 0; i < f.length; i++) {
                final int m = f[i].getModifiers();
                final Transient ta = f[i].getAnnotation(Transient.class);
                if (ta == null) {
                    if (Modifier.isPrivate(m)) {
                        f[i].setAccessible(true);
                    }
                    vs.add(f[i].get(entity));
                }
            }
            dcs = new ValueSet(vs.toArray(new Object[]{}));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return dcs;
    }

    private ValueSet getDcsFromBytes() {
        if (this.chunk == null) {
            throw new RuntimeException("chunk of bytes not exists");
        }

        try {
            Field[] cs = null;
            if (t != null) {
                cs = t.getFields();
            } else {
                Field[] f = class_.getDeclaredFields();
                List<Field> ff = new ArrayList<Field>();
                for (int i = 0; i < f.length; i++) {
                    int m = f[i].getModifiers();
                    Transient ta = f[i].getAnnotation(Transient.class);
                    if (ta == null) {
                        ff.add(f[i]);
                    }
                }
                cs = ff.toArray(new Field[]{});
            }

            final ByteString bs = new ByteString(this.chunk);
            final ValueSet dcs = new ValueSet(new Object[cs.length]);
            int s = 0;

            for (int i = 0; i < cs.length; i++) {
                final int v = Types.isVarType(cs[i]) ? 4 : 0;
                final int m = cs[i].getModifiers();
                final Id a = cs[i].getAnnotation(Id.class);
                //All var length types is non-primitive
                final byte[] data = bs.substring(s + v, s + v + (Types.isVarType(cs[i]) ? bs.getIntFromBytes(bs.substring(s, s + v)) : Types.getLength(cs[i])));
                if (a != null) {
                    serializedId = data;
                }
                try {
                    if (Modifier.isPrivate(m)) {
                        cs[i].setAccessible(true);
                    }
                    dcs.getValueSet()[i] = sr.deserialize(data, cs[i]);
                    if (a != null) {
                        id = (Comparable) dcs.getValueSet()[i];
                    }
                } catch (UnsupportedEncodingException e) {
                    dcs.getValueSet()[i] = "UnsupportedEncodingException";
                }
                s = s + data.length + v;
            }

            return dcs;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public UndoChunk getUndoChunk() {
        if (header==null) {
            return null;
        }
        if (header.getTran()==null) {
            return null;
        }
        return uc;
    }

    public void setUndoChunk(UndoChunk uc) {
        this.uc = uc;
    }

    public Table getT() {
        return t;
    }

    protected void setT(Table t) {
        this.t = t;
    }

    public Comparable getId (Field idfield, Session s) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        if (serializedId==null) {
            if (entity==null) {
                getEntity();
            }
            Class c = entity.getClass();
            final TransEntity ta = (TransEntity)c.getAnnotation(TransEntity.class);
            final SystemEntity sa = (SystemEntity)c.getAnnotation(SystemEntity.class);
            if (ta!=null) {
                //for Transactional Wrapper Entity we must get superclass (original Entity class)
                c = c.getSuperclass();
            }
            if (idfield == null) {
                Field[] f = c.getDeclaredFields();
                for (int i = 0; i < f.length; i++) {
                    final Id a = f[i].getAnnotation(Id.class);
                    if (a != null) {
                        if (sa != null) {
                            id = (Comparable) f[i].get(entity);
                        } else {
                            Method z = c.getMethod("get" + f[i].getName().substring(0, 1).toUpperCase() + f[i].getName().substring(1, f[i].getName().length()), new Class<?>[]{Session.class});
                            Object v = z.invoke(entity, new Object[]{s});
                            id = (Comparable) v;
                        }
                    }
                }
            } else {
                if (sa != null) {
                    id = (Comparable) idfield.get(entity);
                } else {
                    Method z = c.getMethod("get" + idfield.getName().substring(0, 1).toUpperCase() + idfield.getName().substring(1, idfield.getName().length()), new Class<?>[]{Session.class});
                    Object v = z.invoke(entity, new Object[]{s});
                    id = (Comparable) v;
                }
            }
        }
        return id;
    }

    public byte[] getSerializedId (Session s) throws InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException, IllegalAccessException, UnsupportedEncodingException, InternalException {
        if (serializedId==null) {
            if (entity==null) {
                getEntity();
            }
            Class c = entity.getClass();
            final TransEntity ta = (TransEntity)c.getAnnotation(TransEntity.class);
            final SystemEntity sa = (SystemEntity)c.getAnnotation(SystemEntity.class);
            if (ta!=null) {
                //for Transactional Wrapper Entity we must get superclass (original Entity class)
                c = c.getSuperclass();
            }
            Field[] f = c.getDeclaredFields();
            for (int i=0; i<f.length; i++) {
                final Id a = f[i].getAnnotation(Id.class);
                if (a!=null) {
                    if (sa!=null) {
                        Method z = c.getMethod("get"+f[i].getName().substring(0,1).toUpperCase()+f[i].getName().substring(1,f[i].getName().length()), null);
                        Object v = z.invoke(entity, null);
                        id = (Comparable) v;
                        serializedId = sr.serialize(f[i].getType().getName(), v);
                    } else {
                        Method z = c.getMethod("get"+f[i].getName().substring(0,1).toUpperCase()+f[i].getName().substring(1,f[i].getName().length()), new Class<?>[]{Session.class});
                        Object v = z.invoke(entity, new Object[]{s});
                        id = (Comparable) v;
                        serializedId = sr.serialize(f[i].getType().getName(), v);
                    }
                }
            }
        }
        return serializedId;
    }
    
    public int getBytesAmount() {
        return this.getHeader().getLen() + this.getHeader().getHeaderSize();
    }

    public int compareTo (Object o) {
        return this.getDcs().compareTo(((DataChunk)o).getDcs());
    }

    //used in partial compare datachunks in sql group algorithm
    //thr = threshold for set (actual fields amount)
    public int compare (Object o, int thr) {
        if (thr==0) {
            return 0; //group fields not exists
        }
        return this.getDcs().compare(((DataChunk)o).getDcs(), thr);
    }

    public DataChunk () {

    }

    //for index implementation
    public DataChunk (ValueSet vs, Session s, RowId r, Table t) throws ClassNotFoundException, IllegalAccessException, UnsupportedEncodingException, InternalException, InstantiationException {
        this.state = INIT_STATE;
        this.t = t;
        final ByteString res = new ByteString();
        final Field[] f = t.getFields();
        for (int i=0; i<f.length; i++) {
            byte[] b = sr.serialize(f[i].getType().getName(), vs.getValueSet()[i]);
            if (b==null) { b = new byte[]{}; } //stub for reflection convert null fields to byte[] object in DataRecord
            if (Types.isVarType(f[i])) {
                res.addBytesFromInt(b.length);
                res.append(b);
            } else {
                res.append(b);
            }
        }
        chunk = res.getBytes();
        this.header = new RowHeader(r, s.getTransaction(), chunk.length, false);
    }

    //group implementation
    public DataChunk (ValueSet vs, Session s, Table t) {
        this.header = new RowHeader(null, s.getTransaction(), 0, false);
        this.state = NORMAL_STATE;
        this.t = t;

        try {
            entity = t.getInstance(); //returns empty instance
            final Field[] cs = t.getFields();
            for (int i=0; i<cs.length; i++) {
                final int m = cs[i].getModifiers();
                if (Modifier.isPrivate(m)) {
                    cs[i].setAccessible(true);
                }
                if (vs.getValueSet()[i]!=null) {
                    cs[i].set(entity, vs.getValueSet()[i]);
                }
            }
            if (!t.isNoTran()) {
                ((EntityContainer)entity).setRowId(header.getRowID());
                ((EntityContainer)entity).setTran(header.getTran());
                ((EntityContainer)entity).setDataChunk(this);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        header.setLen(getChunk().length);
        //header.setLen(getChunkLen());
    }

    //serializer INSERT ONLY!!! (with generate Id value)
    public DataChunk (Object o, Session s) {
        this(o, s, null);
    }

    //serializer INSERT ONLY!!! (with generate Id value) - rowid for index chunk
    public DataChunk (Object o, Session s, RowId r) {
        this.entity = o;
        this.state = NORMAL_STATE;
        this.header = new RowHeader(r, null, getChunk().length, false);
    }

    public DataChunk (byte[] b, int file, long frame, int hsize, Table t, Class c)  {
        final ByteString bs = new ByteString(b);
        this.t = t;
        this.class_= c;
        this.header = new RowHeader(bs.substring(0, hsize), file, frame);
        this.state = INIT_STATE;
        final ByteString bsc = new ByteString(bs.substring(hsize, b.length));
        this.chunk = bsc.getBytes();
    }

    //constructor for clone method - de-serialize chunk only without header 
    public DataChunk (byte[] b, Table t, RowHeader h, DataChunk source) {
        this.chunk  = b;
        this.header = h;
        this.state = INIT_STATE;
        this.t = t;
    }

    public byte[] getChunk () {
        if (state == INIT_STATE) {
            return chunk;
        }
        if (state == NORMAL_STATE) {
            try {
                if (this.entity == null) {
                    throw new InternalException();
                }
                Class c = this.entity.getClass();
                final SystemEntity sa = (SystemEntity) c.getAnnotation(SystemEntity.class);
                final TransEntity ta = (TransEntity) c.getAnnotation(TransEntity.class);
                Entity ea = (Entity) c.getAnnotation(Entity.class);
                if (ta != null) {
                    //for Transactional Wrapper Entity we must get superclass (original Entity class)
                    c = c.getSuperclass();
                    ea = (Entity) c.getAnnotation(Entity.class);
                }
                final Field[] f = c.getDeclaredFields();
                final ByteString res = new ByteString();
                if (ea == null) {
                    throw new InternalException();
                }
                if (t != null) {
                    if (!t.getName().equals(c.getName())) {
                        throw new InternalException();
                    }
                }
                for (int i = 0; i < f.length; i++) {
                    final int m = f[i].getModifiers();
                    final Transient tr = f[i].getAnnotation(Transient.class);
                    if (tr == null) {
                        byte[] b;
                        if (Modifier.isPrivate(m)) {
                            f[i].setAccessible(true);
                        }
                        b = getBytes(f[i], this.entity);
                        if (b == null) {
                            b = new byte[]{};
                        } //stub for reflection convert null fields to byte[] object in DataRecord
                        if (Types.isVarType(f[i])) {
                            res.addBytesFromInt(b.length);
                            res.append(b);
                        } else {
                            res.append(b);
                        }
                    }
                }

                return res.getBytes();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    //todo
    public int getChunkLen () {
        if (state == INIT_STATE) {
            return chunk.length;
        }
        if (state == NORMAL_STATE) {
            try {
                if (this.entity == null) {
                    throw new InternalException();
                }
                Class c = this.entity.getClass();
                final SystemEntity sa = (SystemEntity) c.getAnnotation(SystemEntity.class);
                final TransEntity ta = (TransEntity) c.getAnnotation(TransEntity.class);
                Entity ea = (Entity) c.getAnnotation(Entity.class);
                if (ta != null) {
                    //for Transactional Wrapper Entity we must get superclass (original Entity class)
                    c = c.getSuperclass();
                    ea = (Entity) c.getAnnotation(Entity.class);
                }
                final Field[] f = c.getDeclaredFields();
                final AtomicInteger res = new AtomicInteger();
                if (ea == null) {
                    throw new InternalException();
                }
                if (t != null) {
                    if (!t.getName().equals(c.getName())) {
                        throw new InternalException();
                    }
                }
                for (int i = 0; i < f.length; i++) {
                    final int m = f[i].getModifiers();
                    final Transient tr = f[i].getAnnotation(Transient.class);
                    if (tr == null) {
                        if (Modifier.isPrivate(m)) {
                            f[i].setAccessible(true);
                        }
                        final int len  = getLen(f[i], this.entity);
                        if (Types.isVarType(f[i])) {
                            res.getAndAdd(4);
                            res.getAndAdd(len);
                        } else {
                            res.getAndAdd(len);
                        }
                    }
                }

                return res.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return 0;
    }

    public Object getExistingEntity() {
        return this.entity;
    }

    public Object getEntity () {
        if (entity==null) {
            try {
                final ValueSet dcs = getDcsFromBytes();
                final Object o = t.getInstance(); //returns empty instance
                final Field[] cs = t.getFields();
                for (int i=0; i<cs.length; i++) {
                    final int m = cs[i].getModifiers();
                    if (Modifier.isPrivate(m)) {
                        cs[i].setAccessible(true);
                    }
                    if (dcs.getValueSet()[i]!=null) {
                        cs[i].set(o, dcs.getValueSet()[i]);
                    }
                }
                if (t.isIndex()) {
                    final ResultSetEntity rsa = (ResultSetEntity) ((Table) this.t).getTableClass().getAnnotation(ResultSetEntity.class);
                    final DataChunk dc = rsa == null ? (DataChunk) Instance.getInstance().getChunkByPointer(this.getHeader().getFramePtr(), this.getHeader().getFramePtrRowId().getRowPointer()) : this;
                    if (dc == null) {
                        // todo during rframe.IndexFrame.init system directory not yet contains replicated FrameData objects
                        logger.error("null datachunk found");
                    }
                    dc.setIc(this);
                    ((IndexChunk)o).setFramePtrRowId(this.getHeader().getFramePtrRowId());
                    ((IndexChunk)o).setDataChunk(dc);
                }
                if (!t.isNoTran()) {
                    ((EntityContainer) o).setRowId(header.getRowID());
                    ((EntityContainer) o).setTran(header.getTran());
                    if (!t.isIndex()) {
                        ((EntityContainer) o).setDataChunk(this);
                    }
                }
                entity = o;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.chunk = null;
        this.state = NORMAL_STATE;
        return entity;
    }

    public Object getStandaloneEntity() {
        try {
            final ValueSet dcs = getDcs();
            final Object o = t.getInstance(); //returns empty instance
            final Field[] cs = t.getFields();
            for (int i=0; i<cs.length; i++) {
                final int m = cs[i].getModifiers();
                if (Modifier.isPrivate(m)) {
                    cs[i].setAccessible(true);
                }
                if (dcs.getValueSet()[i]!=null) {
                    cs[i].set(o, dcs.getValueSet()[i]);
                }
            }
            if (!t.isNoTran()) {
                ((EntityContainer) o).setRowId(header.getRowID());
                ((EntityContainer) o).setTran(header.getTran());
                if (!t.isIndex()) {
                    ((EntityContainer) o).setDataChunk(this);
                }
            }
            return o;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //for bootstrap system / not use table objects
    public Object getEntity (Class c, Object[] params) {
        SystemEntity ca = (SystemEntity)c.getAnnotation(SystemEntity.class);
        try {
            if (ca!=null) { //System non-transactional
                Object o = null;
                if (params!=null) {
                    final Class<?>[] cs = new Class<?>[params.length];
                    for (int i=0; i<params.length; i++) {
                        cs[i] = params[i].getClass();
                    }
                    final Constructor cr = c.getConstructor(cs);
                    o = cr.newInstance(params);
                } else {
                    o = c.newInstance();
                }
                final java.lang.reflect.Field[] f = c.getDeclaredFields();
                int x = 0;
                for (int i=0; i<f.length; i++) {
                    final Transient ta = f[i].getAnnotation(Transient.class);
                    if (ta==null) {
                        final int m = f[i].getModifiers();
                        if (Modifier.isPrivate(m)) {
                            f[i].setAccessible(true);
                        }
                        f[i].set(o, getDcs().getValueSet()[x]);
                        x++;
                    }
                }
                entity = o;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.chunk = null;
        this.state = NORMAL_STATE;
        return entity;
    }

    public void setFrameData(FrameData b) {
        entity = b;
    }

    @Deprecated
    public Object getUndoEntity () {
        if (undoentity==null) {
            try {
                final ClassLoader cl = this.getClass().getClassLoader();
                final Class c = cl.loadClass(t.getName());
                final Object o = c.newInstance();
                final Field[] cs = t.getFields();
                for (int i=0; i<cs.length; i++) {
                    final int m = cs[i].getModifiers();
                    if (Modifier.isPrivate(m)) {
                        cs[i].setAccessible(true);
                    }
                    cs[i].set(o, getDcs().getValueSet()[i]);
                }
                undoentity = o;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return undoentity;
    }

    public void updateEntity(Object o) throws InternalException, IllegalAccessException {
        final Object oo = this.getEntity();
        if (!oo.getClass().getName().equals(o.getClass().getName())) {
            throw new InternalException(); //classes of object do not match
        }
        if (oo==o) {
            throw new InternalException(); //same object, not need to update
        }

        if (this.t==null) {
            if (Instance.getInstance().getSystemState()==Instance.SYSTEM_STATE_UP) {
                this.t = Instance.getInstance().getTableByName(oo.getClass().getName());
                if (this.t==null) {
                    throw new InternalException();
                }
            }
        }
        final Field[] cs = t.getFields();

        for (int i=0; i<cs.length; i++) {
            int m = cs[i].getModifiers();
            if (Modifier.isPrivate(m)) {
                cs[i].setAccessible(true);
            }
            cs[i].set(oo, cs[i].get(o));
        }
    }

    protected void setNormalState() {
        //set chunk to normal state after update
        this.chunk = null;
        this.state = NORMAL_STATE;
    }

    //for UNDO processing
    public DataChunk cloneEntity(Session s) throws InternalException {
        final byte[] b = this.getChunk();
        final DataChunk dc_ = new DataChunk(b, this.t, new RowHeader(this.getHeader()), this);
        final Object entity_ = this.getEntity();
        if (this.t == null) {
            dc_.t = Instance.getInstance().getTableByName(entity_.getClass().getName());
        }
        dc_.class_ = entity_.getClass();
        return dc_;
    }

    public DataChunk restore(UndoChunk uc) throws Exception {
        // since we are using a dirty hack with changing the header in a source chunk for rollback his,
        // we need to first delete the source chunk in the target block to prevent inconsistency within ChunkMap / chunk headers
        if (!(this.getHeader().getRowID().getFileId() == uc.getFile() && this.getHeader().getRowID().getFramePointer() == uc.getFrame())) {
            final long srcFrameId = uc.getFile() + uc.getFrame();
            final Frame srcFrame = Instance.getInstance().getFrameById(srcFrameId).getFrame();
            srcFrame.removeChunk(uc.getPtr(), null, true);
        }
        return this;
    }

    private byte[] getBytes(Field f, Object o) throws IllegalAccessException, UnsupportedEncodingException, ClassNotFoundException, InternalException, InstantiationException {
        final String t = f.getType().getName();
        final Object fo = f.get(o);
        return sr.serialize(t, fo);
    }

    private int getLen(Field f, Object o) throws IllegalAccessException, UnsupportedEncodingException, ClassNotFoundException, InternalException, InstantiationException {
        final String t = f.getType().getName();
        final Object fo = f.get(o);
        return sr.length(t, fo);
    }

    public RowHeader getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = (RowHeader)header;
    }

    //lock mechanism

    private synchronized DataChunk insertUC (UndoChunk uc, Session s, LLT llt) throws Exception {
        final WaitFrame ubw = s.getTransaction().getAvailableFrame(uc, true);
        final FrameData ub = ubw.getBd();
        if (ub == null) {
            throw new InternalException();
        }

        final DataChunk dc = new DataChunk(uc, s);
        final int p = ub.getDataFrame().insertChunk(dc, s, true, llt);
        if (p == 0) {
            final Table t = Instance.getInstance().getTableByName(UndoChunk.class.getName());
            final FrameData nb = t.createNewFrame(ub, ub.getFile(), 0, 0, false, false, false, s, llt);
            s.getTransaction().setNewLB(ub, nb, false);
            nb.getDataFrame().insertChunk(dc, s, true, llt);
            dc.uframe = nb;
        } else {
            dc.uframe = ub;
        }
        ubw.release();
        return dc;
    }

    //for uc only
    public void setEntity(Object entity) {
        if (entity instanceof UndoChunk) {
            this.entity = entity;
        }
    }

    protected void setEntity(Object entity, boolean z) {
        this.entity = entity;
    }

    //method for transactional objects ONLY
    public synchronized DataChunk lock (Session s, LLT llt) throws Exception {
        final Object o = this.getEntity();
        final Table t = Instance.getInstance().getTableByName(o.getClass().getName());
        if (t.isNoTran()) {
            throw new InternalException();
        }
        if (((EntityContainer)o).getTran() != null) {
            if (((EntityContainer)o).getTran().getTransId() == s.getTransaction().getTransId()) {
                return null; //object already locked by this transaction
            }
        }

        if (s.getTransaction().getTransId() != this.getHeader().getTran().getTransId()) {
            boolean trylock = true;
            int cnt = 0;
            while (trylock) {
                cnt++;
                if (this.getHeader().getTran().getCid() == 0) {
                    if (cnt == 3) {
                        throw new CannotAccessToLockedRecord();
                    }
                    Thread.yield();
                    Thread.sleep(1000);
                } else {
                    trylock = false;
                }
            }
        }

        //lock record
        this.getHeader().setTran(s.getTransaction());
        ((EntityContainer)o).setTran(s.getTransaction());

        final DataChunk cc = this.cloneEntity(s);
        cc.getHeader().setTran(s.getTransaction());
        final RowId rw = this.getHeader().getRowID();
        uc = new UndoChunk(cc, s.getTransaction(), rw.getFileId(), rw.getFramePointer(), rw.getRowPointer());
        return insertUC(uc, s, llt);
    }

    public FrameData getUframe() {
        return uframe;
    }

    public boolean isTerminate() {
        return terminate;
    }

    public void setTerminate(boolean terminate) {
        this.terminate = terminate;
    }

    public void setIc(DataChunk ic) {
        final Table t_ = ic.t == null ? Instance.getInstance().getTableByName(ic.getEntity().getClass().getName()) : ic.t;
        this.ics.put(t_.getObjectId(), ic);
    }

    public void clearIcs() {
        this.ics.clear();
    }

    protected void cleanUpIcs() {
        for (Map.Entry<Integer, DataChunk> entry : this.ics.entrySet()) {
            final IndexChunk ic = (IndexChunk) entry.getValue().getEntity();
            ic.setDataChunk(null);
        }
    }

    public DataChunk getIc(IndexDescript ids, Session s) throws Exception {
        final Table ixt = Instance.getInstance().getTableByName(SYSTEM_PKG_PREFIX + ids.getName());
        final DataChunk ic = this.ics.get(ixt.getObjectId());
        if (ic == null) {
            final ValueSet key = this.getValueByColumnName(ids.getColumns(), s);
            final DataChunk ic_ = ixt.getObjectByKey(key, s);
            if (ic_ != null) {
                this.ics.put(ixt.getObjectId(), ic_);
                return ic_;
            } else {
                throw new RuntimeException("Unable to retrieve index chunk: " + ids.getName());
            }
        }
        return ic;
    }

    public ValueSet getValueByColumnName(String[] columns, Session s) throws Exception {
        final Object o = this.getEntity();
        final Class c = o.getClass();
        final SystemEntity sa = (SystemEntity)c.getAnnotation(SystemEntity.class);
        final List<Object> res = new ArrayList<>();
        for (String name : columns) {
            if (sa != null) {
                final Method z = c.getMethod("get" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length()), null);
                res.add(z.invoke(o, null));
            } else {
                final Method z = c.getMethod("get" + name.substring(0, 1).toUpperCase() + name.substring(1, name.length()), new Class<?>[]{Session.class});
                res.add(z.invoke(o, new Object[]{s}));
            }
        }
        return new ValueSet(res.toArray(new Object[]{}));
    }

    private byte[] getBytesFromHexString(String s) {
        byte b[] = new byte[s.length()/2];
        for (int i=0; i<s.length(); i+=2) {
            int x = Integer.parseInt(s.substring(i,i+2),16);
            b[i/2] = (byte)x;
        }
        return b;
    }

    public synchronized String getHexByChunk() {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<chunk.length; i++) {
            String s = Integer.toHexString(chunk[i]);
            if (s.length()==1) {sb.append("0"); sb.append(s);}
            if (s.length()==2) {sb.append(s);}
            if (s.length()==8) {sb.append(s.substring(6,8));}
        }
        return sb.toString();
    }

}
