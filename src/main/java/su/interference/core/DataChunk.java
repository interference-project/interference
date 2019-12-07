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
import su.interference.exception.InternalException;
import su.interference.exception.CannotAccessToLockedRecord;
import su.interference.persistent.*;
import su.interference.persistent.Table;
import su.interference.serialize.ByteString;
import su.interference.serialize.CustomSerializer;

import javax.persistence.*;
import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.*;
import java.net.MalformedURLException;
import java.text.ParseException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@SuppressWarnings("unchecked")
public class DataChunk implements Chunk {

    private final static Logger logger = LoggerFactory.getLogger(DataChunk.class);
    private DataObject t;
    private volatile RowHeader header;
    //cache-dependency parameters
    private volatile byte[] chunk;
    private volatile ValueSet dcs; //datacolumn set
    private byte[] id;
    private Object entity;
    private Object undoentity;
    private DataChunk source;
    private UndoChunk uc;
    private final CustomSerializer sr = new CustomSerializer();

    //returns datacolumn set
    public ValueSet getDcs() throws ClassNotFoundException, IllegalAccessException, InternalException, MalformedURLException {
        if (dcs==null) {
            final Field[] f = t.getFields();
            final Object[] vs = new Object[f.length];
            for (int i=0; i<f.length; i++) {
                final int m = f[i].getModifiers();
                final Transient ta = f[i].getAnnotation(Transient.class);
                if (ta==null) {
                    if (Modifier.isPrivate(m)) {
                        f[i].setAccessible(true);
                    }
                    vs[i] = f[i].get(entity);
                }
            }
            dcs = new ValueSet(vs);
        }
        return dcs;
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

    //optimistic method used in DataChunk.compareTo method
    public ValueSet getDcs(boolean z) {
        return this.dcs;
    }

    public DataObject getT() {
        return t;
    }

    public byte[] getId (Session s) throws InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException, IllegalAccessException, UnsupportedEncodingException, InternalException {
        if (id==null) {
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
                        id = sr.serialize(f[i].getType().getName(), v);
                    } else {
                        Method z = c.getMethod("get"+f[i].getName().substring(0,1).toUpperCase()+f[i].getName().substring(1,f[i].getName().length()), new Class<?>[]{Session.class});
                        Object v = z.invoke(entity, new Object[]{s});
                        id = sr.serialize(f[i].getType().getName(), v);
                    }
                }
            }
        }
        return id;
    }
    
    public int getBytesAmount() {
        return this.getChunk().length+this.getHeader().getHeader().length;
    }

    public int compareTo (Object o) {
        return this.dcs.compareTo(((DataChunk)o).getDcs(false));
    }

    //used in partial compare datachunks in sql group algorithm
    //thr = threshold for set (actual fields amount)
    public int compare (Object o, int thr) {
        if (thr==0) {
            return 0; //group fields not exists
        }
        return this.dcs.compare(((DataChunk)o).getDcs(false), thr);
    }

    public DataChunk () {

    }

    //for index implementation
    public DataChunk (ValueSet vs, Session s, RowId r, Table t) throws ClassNotFoundException, IllegalAccessException, UnsupportedEncodingException, InternalException, MalformedURLException, InstantiationException {
        this.dcs = vs;
        this.header = new RowHeader(r, s.getTransaction(), 0, false);
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
    }

    //serializer INSERT ONLY!!! (with generate Id value)
    public DataChunk (Object o, Session s) throws IOException, InvocationTargetException, NoSuchMethodException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        this(o, s, null);
    }

    //serializer INSERT ONLY!!! (with generate Id value) - rowid for index chunk
    public DataChunk (Object o, Session s, RowId r) throws IOException, InvocationTargetException, NoSuchMethodException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.entity = o;
        this.header = new RowHeader(r, null, 0, false);
        Class c = o.getClass();
        final SystemEntity sa = (SystemEntity)c.getAnnotation(SystemEntity.class);
        final TransEntity ta = (TransEntity)c.getAnnotation(TransEntity.class);
        final IndexEntity xa = (IndexEntity)c.getAnnotation(IndexEntity.class);
        if (ta!=null) {
            //for Transactional Wrapper Entity we must get superclass (original Entity class)
            c = c.getSuperclass();
        }
//        long gid = 0;
        final ByteString res = new ByteString();
        final Entity ea = (Entity)c.getAnnotation(Entity.class);
        if (ea==null) {
            throw new InternalException();
        }
        if (Instance.getInstance().getSystemState()==Instance.SYSTEM_STATE_UP) {
            this.t = Instance.getInstance().getTableByName(c.getName());
            if (this.t==null&&!c.getName().equals("su.interference.core.SystemData")) {
                throw new InternalException();
            }
        }
        final Field[] f = c.getDeclaredFields();
        final ArrayList<Object> dcsl = new ArrayList<Object>();
        for (int i=0; i<f.length; i++) {
            final int m = f[i].getModifiers();
            final Transient tr = f[i].getAnnotation(Transient.class);
            if (tr==null) {
                byte[] b;
                if (Modifier.isPrivate(m)) {
                    f[i].setAccessible(true);
                }
                if (xa!=null) { //dcs use in datachunk.compareTo / indexes
                    dcsl.add(f[i].get(o));
                }
                b = getBytes(f[i], o);
                if (b==null) { b = new byte[]{}; } //stub for reflection convert null fields to byte[] object in DataRecord
                if (Types.isVarType(f[i])) {
                    res.addBytesFromInt(b.length);
                    res.append(b);
                } else {
                    res.append(b);
                }
            }
        }
        if (xa!=null) { //dcs use in datachunk.compareTo / indexes
            this.dcs = new ValueSet(dcsl.toArray(new Object[]{}));
        }
        for (int i=0; i<f.length; i++) {
            final Id a = f[i].getAnnotation(Id.class);
            if (a!=null) {
                if (sa!=null) {
                    final Method z = c.getMethod("get"+f[i].getName().substring(0,1).toUpperCase()+f[i].getName().substring(1,f[i].getName().length()), null);
                    final Object v = z.invoke(o, null);
                    id = sr.serialize(f[i].getType().getName(), v);
                } else {
                    final Method z = c.getMethod("get"+f[i].getName().substring(0,1).toUpperCase()+f[i].getName().substring(1,f[i].getName().length()), new Class<?>[]{Session.class});
                    final Object v = z.invoke(o, new Object[]{s});
                    id = sr.serialize(f[i].getType().getName(), v);
                }
            }
        }
        chunk = res.getBytes();
    }

    // ************************************************************************
    // main disk-> constructor, called from DataFrame & IndexFrame constructors
    // ************************************************************************
    public DataChunk (byte[] b, int file, long frame, int hsize, DataObject t, Class c) throws ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException {
        final ByteString bs = new ByteString(b);
        this.t = t;
        this.header = new RowHeader(bs.substring(0, hsize), file, frame);
        final ByteString bsc = new ByteString(bs.substring(hsize, b.length));
        this.chunk = bsc.getBytes();

//        Field[] cs = t.getColumns();

        Field[] cs = null;
        if (t!=null) {
            cs = t.getFields();
        } else {
            Field[] f = c.getDeclaredFields();
            List<Field> ff = new ArrayList<Field>();
            for (int i=0; i<f.length; i++) {
                int m = f[i].getModifiers();
                Transient ta = f[i].getAnnotation(Transient.class);
                if (ta==null) {
                    ff.add(f[i]);
                }
            }
            cs = ff.toArray(new Field[]{});
        }

        dcs = new ValueSet(new Object[cs.length]);
        int s = 0;

        for (int i=0; i<cs.length; i++) {
            final int v = Types.isVarType(cs[i])?4:0;
            final int m = cs[i].getModifiers();

            //All var length types is non-primitive
            final byte[] data = bsc.substring(s+v, s+v+(Types.isVarType(cs[i])?bsc.getIntFromBytes(bsc.substring(s,s+v)):Types.getLength(cs[i])));
            try {
                if (Modifier.isPrivate(m)) {
                    cs[i].setAccessible(true);
                }
                dcs.getValueSet()[i] = sr.deserialize(data, cs[i]);
            } catch (UnsupportedEncodingException e) {
                dcs.getValueSet()[i] = "UnsupportedEncodingException";
            }
            s = s + data.length + v;
        }   
    }

    //constructor for clone method - de-serialize chunk only without header 
    public DataChunk (byte[] b, DataObject t, RowHeader h, DataChunk source) throws ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException {
        final ByteString bs = new ByteString(b);
        this.chunk  = b;
        this.header = h;
        this.source = source;
//        Field[] cs = t.getColumns();
        this.t = t;
        final Field[] cs = t.getFields();
        dcs = new ValueSet(new Object[cs.length]);
        int s = 0;

        for (int i=0; i<cs.length; i++) {
            final int v = Types.isVarType(cs[i])?4:0;
            final int m = cs[i].getModifiers();
            final Id a  = cs[i].getAnnotation(Id.class);
            //All var length types is non-primitive
            final byte[] data = bs.substring(s+v, s+v+(Types.isVarType(cs[i])?bs.getIntFromBytes(bs.substring(s,s+v)):Types.getLength(cs[i])));
            if (a!=null) {
                id = data;
            }
            try {
                if (Modifier.isPrivate(m)) {
                    cs[i].setAccessible(true);
                }
                dcs.getValueSet()[i] = sr.deserialize(data, cs[i]);
            } catch (UnsupportedEncodingException e) {
                dcs.getValueSet()[i] = "UnsupportedEncodingException";
            }
            s = s + data.length + v;
        }
    }

    //constructor for remote method - de-serialize chunk from hex string
    public DataChunk (String h, FrameData bd) throws ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException {
        byte[] b = getBytesFromHexString(h);
        final ByteString bs = new ByteString(b);
        this.chunk = b;
        this.t = bd.getDataObject();
        final Field[] cs = t.getFields();
        dcs = new ValueSet(new Object[cs.length]);
        int s = 0;

        for (int i=0; i<cs.length; i++) {
            final int v = Types.isVarType(cs[i])?4:0;
            final int m = cs[i].getModifiers();
            final Id a  = cs[i].getAnnotation(Id.class);
            //All var length types is non-primitive
            final byte[] data = bs.substring(s+v, s+v+(Types.isVarType(cs[i])?bs.getIntFromBytes(bs.substring(s,s+v)):Types.getLength(cs[i])));
            if (a!=null) {
                id = data;
            }
            try {
                if (Modifier.isPrivate(m)) {
                    cs[i].setAccessible(true);
                }
                dcs.getValueSet()[i] = sr.deserialize(data, cs[i]);
            } catch (UnsupportedEncodingException e) {
                dcs.getValueSet()[i] = "UnsupportedEncodingException";
            }
            s = s + data.length + v;
        }
    }

    //constructor for remote method - de-serialize chunk from hex string
    public DataChunk (String h, Table t, Transaction tx) throws ClassNotFoundException, InstantiationException, IllegalAccessException, InternalException, MalformedURLException {
        byte[] b = getBytesFromHexString(h);
        final ByteString bs = new ByteString(b);
        this.chunk = b;
        this.header = new RowHeader(new RowId(0,0,0), tx, 0, false);
        this.t = t;
//        Field[] cs = t.getColumns();
        final Field[] cs = t.getFields();
        dcs = new ValueSet(new Object[cs.length]);
        int s = 0;

        for (int i=0; i<cs.length; i++) {
            final int v = Types.isVarType(cs[i])?4:0;
            final int m = cs[i].getModifiers();
            final Id a  = cs[i].getAnnotation(Id.class);

            //All var length types is non-primitive
            final byte[] data = bs.substring(s+v, s+v+(Types.isVarType(cs[i])?bs.getIntFromBytes(bs.substring(s,s+v)):Types.getLength(cs[i])));
            if (a!=null) {
                id = data;
            }
            try {
                if (Modifier.isPrivate(m)) {
                    cs[i].setAccessible(true);
                }
                dcs.getValueSet()[i] = sr.deserialize(data, cs[i]);
            } catch (UnsupportedEncodingException e) {
                dcs.getValueSet()[i] = "UnsupportedEncodingException";
            }
            s = s + data.length + v;
        }
    }

    //for UPDATE ONLY!!!
    public byte[] flush (Session s) throws IOException, InvocationTargetException, NoSuchMethodException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        if (this.entity==null) {
            throw new InternalException();
        }
        //int chl = this.chunk.length;
        Class c = this.entity.getClass();
        final SystemEntity sa = (SystemEntity)c.getAnnotation(SystemEntity.class);
        final TransEntity ta = (TransEntity)c.getAnnotation(TransEntity.class);
        Entity ea = (Entity)c.getAnnotation(Entity.class);
        if (ta!=null) {
            //for Transactional Wrapper Entity we must get superclass (original Entity class)
            c = c.getSuperclass();
            ea = (Entity)c.getAnnotation(Entity.class);
        }
        final Field[] f = c.getDeclaredFields();
        final ByteString res = new ByteString();
        if (ea==null) {
            throw new InternalException();
        }
        if (t!=null) {
            if (!t.getName().equals(c.getName())) {
                throw new InternalException();
            }
        }
        for (int i=0; i<f.length; i++) {
            final int m = f[i].getModifiers();
            final Transient tr = f[i].getAnnotation(Transient.class);
            if (tr==null) {
                byte[] b;
                if (Modifier.isPrivate(m)) {
                    f[i].setAccessible(true);
                }
                b = getBytes(f[i], this.entity);
                if (b==null) { b = new byte[]{}; } //stub for reflection convert null fields to byte[] object in DataRecord
                if (Types.isVarType(f[i])) {
                    res.addBytesFromInt(b.length);
                    res.append(b);
                } else {
                    res.append(b);
                }
            }
        }
        for (int i=0; i<f.length; i++) {
            final Id a = f[i].getAnnotation(Id.class);
            if (a!=null) {
                if (sa!=null) {
                    final Method z = c.getMethod("get"+f[i].getName().substring(0,1).toUpperCase()+f[i].getName().substring(1,f[i].getName().length()), null);
                    final Object v = z.invoke(this.entity, null);
                    id = sr.serialize(f[i].getType().getName(), v);
                } else {
                    final Method z = c.getMethod("get"+f[i].getName().substring(0,1).toUpperCase()+f[i].getName().substring(1,f[i].getName().length()), new Class<?>[]{Session.class});
                    final Object v = z.invoke(this.entity, new Object[]{s});
                    id = sr.serialize(f[i].getType().getName(), v);
                }
            }
        }
        return res.getBytes();
    }

    public Object getEntity () {
        if (entity==null) {
            try {
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
                    final DataChunk dc = (DataChunk) Instance.getInstance().getChunkByPointer(this.getHeader().getFramePtr(), this.getHeader().getFramePtrRowId().getRowPointer());
                    ((IndexChunk)o).setDataChunk(dc);
                    if (dc == null) {
                        final long allocId = Instance.getInstance().getFrameById(this.header.getRowID().getFileId()+this.header.getRowID().getFramePointer()).getAllocId();
                        final long allocId2 = Instance.getInstance().getFrameById(this.getHeader().getFramePtr()).getAllocId();
                        logger.error("null datachunk found for indexframe allocId = " + allocId + " indexptr allocId = " + allocId2);
                    }
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
        return entity;
    }


    public Object getEntityId () { //throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Object id = null;
        try {
            final Field[] cs = t.getFields();
            for (int i=0; i<cs.length; i++) {
                final Id a  = cs[i].getAnnotation(Id.class);
                final int m = cs[i].getModifiers();
                if (Modifier.isPrivate(m)) {
                    cs[i].setAccessible(true);
                }
                if (a!=null) {
                    if (dcs.getValueSet()[i] != null) {
                        id = dcs.getValueSet()[i];
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }

    //TEMPORARY - return Entity without DataChunk for remote operation
    public Object getReceivedEntity () { //throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        if (entity==null) {
            try {
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
                }
                if (!t.isNoTran()) {
                    ((EntityContainer)o).setRowId(header.getRowID());
                    ((EntityContainer)o).setTran(header.getTran());
                    ((EntityContainer)o).setReceived(true);
                }
                entity = o;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return entity;
    }

    //use in group by algorithm for generate another class object with same value set
    public Object getEntity (Table tt) { //throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Object o = null;
        try {
            o = tt.getInstance(); //returns empty instance
            final Field[] cs = tt.getFields();
            for (int i=0; i<cs.length; i++) {
                final int m = cs[i].getModifiers();
                if (Modifier.isPrivate(m)) {
                    cs[i].setAccessible(true);
                }
                if (dcs.getValueSet()[i]!=null) {
                    cs[i].set(o, dcs.getValueSet()[i]);
                }
            }
            if (tt.isIndex()) {

            }
            if (!tt.isNoTran()) {
                ((EntityContainer)o).setRowId(header.getRowID());
                ((EntityContainer)o).setTran(header.getTran());
                ((EntityContainer)o).setDataChunk(this);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return o;
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
                        f[i].set(o, dcs.getValueSet()[x]);
                        x++;
                    }
                }
                entity = o;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entity;
    }

    protected void setTable(Table t) {
        entity = t;
    }

    public void setFrameData(FrameData b) {
        entity = b;
    }

    public Object getUndoEntity () { //throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
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
                    cs[i].set(o, dcs.getValueSet()[i]);
                }
                undoentity = o;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return undoentity;
    }

    public void updateEntity(Object o) throws InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, MalformedURLException {
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

    //for mgmt chunk updates by web form string values
    public void updateEntity(String[] ulist, Session s)
        throws InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException, ParseException {
        final Field[] cs = t.getFields();
        if (cs.length!=ulist.length) {
            throw new InternalException(); //fields amount not match
        }
        for (int i=0; i<cs.length; i++) {
            final Id a = cs[i].getAnnotation(Id.class);
            if (a==null) { //id column do not change
                final int m = cs[i].getModifiers();
                if (Modifier.isPrivate(m)) {
                    cs[i].setAccessible(true);
                }
                cs[i].set(this.entity, sr.deserialize(ulist[i], cs[i].getType().getName()));
            }
        }
    }

    //for UNDO processing
    public DataChunk cloneEntity(Session s) throws IOException, InvocationTargetException, NoSuchMethodException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        final byte[] b = this.flush(s);
        return new DataChunk(b, this.t, new RowHeader(this.getHeader()), this);
    }

    public DataChunk restore(Frame b, Session s) throws Exception {
        if (source == null) {
            throw new InternalException();
        }
        if (!(this.getHeader().getRowID().getFileId() == source.getHeader().getRowID().getFileId() && this.getHeader().getRowID().getFramePointer() == source.getHeader().getRowID().getFramePointer())) {
            final long srcFrameId = source.getHeader().getRowID().getFileId() + source.getHeader().getRowID().getFramePointer();
            final Frame srcFrame = Instance.getInstance().getFrameById(srcFrameId).getFrame();
            //final LLT llt = LLT.getLLT();
            // not need for use llt here
            srcFrame.removeChunk(source.getHeader().getRowID().getRowPointer(), null, true);
            //llt.commit();
        }
        source.setHeader(this.getHeader());
        source.setEntity(this.getEntity(), false);
        source.setChunk(source.flush(s));
        return source;
    }

    private byte[] getBytes(Field f, Object o) throws IllegalAccessException, UnsupportedEncodingException, ClassNotFoundException, InternalException, InstantiationException {
        final String t = f.getType().getName();
        final Object fo = f.get(o);
        return sr.serialize(t, fo);
    }

    public RowHeader getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = (RowHeader)header;
    }

    public byte[] getChunk() {
        return this.chunk;
    }

    public void setChunk(byte[] chunk) {
        this.chunk = chunk;
    }

    //lock mechanism

    private synchronized DataChunk insertUC (FrameData cb, UndoChunk uc, Session s, LLT llt) throws Exception {
        final FrameData ub = s.getTransaction().getAvailableFrame(uc, true);

        if (ub == null) {
            //s.getTransaction().createUndoFrames(s);
            //ub = s.getTransaction().getAvailableFrame(uc, true);
            //if (ub == null) {
                throw new InternalException();
            //}
        }

        final DataChunk dc = new DataChunk(uc, s);
        final int p = ub.getDataFrame().insertChunk(dc, s, true, llt);
        if (p == 0) {
            final Table t = Instance.getInstance().getTableByName("su.interference.persistent.UndoChunk");
            final FrameData nb = t.createNewFrame(ub, ub.getFile(), 0, 0, false, false, false, s, llt);
            s.getTransaction().setNewLB(ub, nb, false);
            nb.getDataFrame().insertChunk(dc, s, true, llt);
            s.getTransaction().storeFrame(cb, nb, 0, s, llt);
        } else {
            s.getTransaction().storeFrame(cb, ub, 0, s, llt);
        }
        ub.release();
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

        final FrameData bd = Instance.getInstance().getFrameById(this.getHeader().getRowID().getFileId() + this.getHeader().getRowID().getFramePointer());
        final DataChunk cc = this.cloneEntity(s);
        cc.getHeader().setTran(s.getTransaction());
        final RowId rw = this.getHeader().getRowID();
        uc = new UndoChunk(cc, s.getTransaction(), rw.getFileId(), rw.getFramePointer(), rw.getRowPointer());
        return insertUC(bd, uc, s, llt);
    }

    protected synchronized void undo(Session s, LLT llt) throws Exception {
        if (s.getTransaction().getTransId() != this.getHeader().getTran().getTransId()) {
            throw new InternalException();
        } else {
            final FrameData bd = Instance.getInstance().getFrameById(this.getHeader().getRowID().getFileId() + this.getHeader().getRowID().getFramePointer());
            final DataChunk cc = this.cloneEntity(s);
            cc.getHeader().setTran(s.getTransaction());
            final RowId rw = this.getHeader().getRowID();
            uc = new UndoChunk(cc, s.getTransaction(), rw.getFileId(), rw.getFramePointer(), rw.getRowPointer());
            insertUC(bd, uc, s, llt);
        }
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
