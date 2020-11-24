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

package su.interference.persistent;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.ByteBuffer;
import java.lang.reflect.*;
import java.lang.annotation.Annotation;
import java.net.MalformedURLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.MapField;
import su.interference.core.*;
import su.interference.exception.*;
import su.interference.metrics.Metrics;
import su.interference.mgmt.MgmtClassIdColumn;
import su.interference.mgmt.MgmtColumn;
import su.interference.sql.ResultSet;

import javax.persistence.*;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

@SuppressWarnings("unchecked")
@Entity
@SystemEntity
@DisableSync
public class Table implements ResultSet {

    @Transient
    private final static Logger logger = LoggerFactory.getLogger(Table.class);
    @Transient
    private final static int IDENT_STORE_SIZE = 1000;
    @Transient
    public static final int CLASS_ID = 1;
    @Transient
    public static final String SYSTEM_PKG_PREFIX = "su.interference.persistent.";

    @Column
    @Id
    @MapColumn
    @GeneratedValue
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int    objectId;
    @Column
    @MapColumn
    @MgmtColumn(width=80, show=true, form=true, edit=false)
    @MgmtClassIdColumn
    private String name;
    @Column
    private int    fileStart;
    @Column
    private long   frameStart;
    @Column
    @MgmtColumn(width=10, show=true, form=false, edit=false)
    private int    frameSize;
    @Column
    private int    fileLast;
    @Column
    private long   frameLast;
    @Column
    private AtomicLong frameAmount = new AtomicLong(0L);
    @Column
    private long   ltran;
    @Column
    private int    parentId;
    @Column
    private int    idIncrement;
    @Column
    private AtomicLong idValue;
    @Column
    private AtomicLong incValue;
    @Column
    private AtomicLong frameOrder;

    @Transient
    private String simpleName;
    @Transient
    private Map<Integer, Long> ixstartfs = new HashMap<>();
    @Transient
    private AtomicLong idValue2;
    @Transient
    private ArrayList<IndexField> indexes;
    @Transient
    private ArrayList<MapField> maps;
    @Transient
    private Class genericClass;
    @Transient
    private Class sc;
    @Transient
    private final boolean index;
    @Transient
    private final boolean notran;
    @Transient
    private final java.lang.reflect.Field idfield;
    @Transient
    private final java.lang.reflect.Method idmethod;
    @Transient
    private final String idfieldtype;
    @Transient
    private final String idfieldgetter;
    @Transient
    private final java.lang.reflect.Field generatedfield;
    @Transient
    private final WaitFrame[] lbs;
    @Transient
    private final AtomicInteger avframeStart = new AtomicInteger(0);
    @Transient
    private final AtomicInteger ixFrameCurr = new AtomicInteger(0);
    @SuppressWarnings("WeakerAccess")
    @Transient
    public java.lang.reflect.Field[] fields;
    @Transient
    public String[] fieldtypes;
    @Transient
    private static final ExecutorService framepool = Executors.newCachedThreadPool();

    public Class getSc() {
        return sc;
    }

    public Class getGenericClass() {
        return genericClass;
    }

    public void setSc(Class sc) {
        this.sc = sc;
    }

    public long getIdValue() {
        return idValue==null?0:idValue.get();
    }

    public long getIncValue() {
        return incValue==null?0:incValue.get();
    }

    public long getFrameOrder() {
        return frameOrder==null?0:frameOrder.get();
    }

    public WaitFrame[] getLbs() {
        return lbs;
    }

    public long getIdValue(Session s, LLT llt) throws Exception {
        if (idValue==null) {
            idValue = new AtomicLong(0);
        }
        if (idIncrement<=1) {
            if (idValue2 == null) {
                idValue2 = new AtomicLong(idValue.get());
                idValue.getAndAdd(IDENT_STORE_SIZE);
                s.persist(this, llt); //update
            }
            if (idValue2.get() == idValue.get()) {
                idValue.getAndAdd(IDENT_STORE_SIZE);
                s.persist(this, llt); //update
            }
            return idValue2.addAndGet(1);
        } else {
            idValue.getAndAdd(idIncrement);
            s.persist(this, llt);
            return idValue.get();
        }
    }

    public synchronized long getIncValue(Session s, LLT llt) throws Exception {
        if (incValue==null) { incValue = new AtomicLong(0); }
        incValue.addAndGet(1);
        s.persist(this, llt); //update
        return incValue.get();
    }

    public synchronized long getFrameOrder(Session s, LLT llt) throws Exception {
        if (frameOrder==null) { frameOrder = new AtomicLong(0); }
        frameOrder.addAndGet(1);
        return frameOrder.get();
    }

    public void setIdValue(AtomicLong idValue) {
        this.idValue = idValue;
    }

    public void setIncValue(AtomicLong incValue) {
        this.incValue = incValue;
    }

    protected void incFrameAmount () {
        frameAmount.incrementAndGet();
    }

    protected  void decFrameAmount () {
        frameAmount.decrementAndGet();
    }

    public static int getCLASS_ID() {
        return CLASS_ID;
    }

    public long getId () {
        return this.objectId;
    }

    public long getLastFrameId() {
        return this.getFileLast()+this.getFrameLast();
    }

    //todo rename to isSystem
    public boolean isNoTran() {
        return this.notran;
    }

    public boolean isIndex() {
        return this.index;
    }

    public boolean isDistributed() {
        final NoDistribute ca = (NoDistribute)this.genericClass.getAnnotation(NoDistribute.class);
        return ca == null;
    }

    public String getSystemName() {
        if (!isNoTran()) {
            if (this.sc!=null) {
                return this.sc.getName();
            }
        }
        return this.name;
    }

    public LinkedBlockingQueue<FrameData> getFrames(Session s) {
        if (this.isIndex()) {
            //get ordered frame sequence
            return this.getLeafFrames(s);
        }
        final LinkedBlockingQueue<FrameData> q = new LinkedBlockingQueue<>(1000);
        final IndexList ixl = Instance.getInstance().getFrameDataTable().getIndexFieldByColumn("objectId").getIndex();
        final int id = this.objectId;
        Runnable r = new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName("interference-retrieve-frames-thread");
                try {
                    for (Object o : ixl.getObjectsByKey(id)) {
                        q.put((FrameData) ((DataChunk) o).getEntity());
                    }
                    //terminate
                    q.put(new FrameData());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        framepool.submit(r);
        return q;
    }

    public LinkedBlockingQueue<FrameData> getFrames() {
        final LinkedBlockingQueue<FrameData> q = new LinkedBlockingQueue<>(1000);
        final IndexList ixl = Instance.getInstance().getFrameDataTable().getIndexFieldByColumn("objectId").getIndex();
        final int id = this.objectId;
        Runnable r = new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName("interference-retrieve-frames-thread");
                try {
                    for (Object o : ixl.getObjectsByKey(id)) {
                        q.put((FrameData) ((DataChunk) o).getEntity());
                    }
                    //terminate
                    q.put(new FrameData());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        framepool.submit(r);
        return q;
    }

    //used in Session.registerTable
    protected synchronized Object newInstance() throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        return newInstance(new Object[]{});
    }

    protected synchronized Object newInstance(Session s) throws Exception {
        return newInstance(new Object[]{}, s);
    }

    //used in Session.newEntity
    public Object newInstance(Object[] params) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        final Class<?>[] cs = new Class<?>[params.length];
        for (int i=0; i<params.length; i++) {
            cs[i] = params[i].getClass();
        }

        final SystemEntity ca = (SystemEntity)this.getClass().getAnnotation(SystemEntity.class);
        if (ca!=null) { //System non-transactional
            return this.getClass().getConstructor(cs).newInstance(params);
        }

        return null;
    }

    //used in Session.newEntity
    @SuppressWarnings("unchecked")
    public Object newInstance(Object[] params, Session s) throws Exception {
        final Class<?>[] cs = new Class<?>[params.length];
        for (int i=0; i<params.length; i++) {
            cs[i] = params[i].getClass();
        }

        if (this.sc!=null) {
            final SystemEntity ca = (SystemEntity)this.sc.getAnnotation(SystemEntity.class);
            if (ca==null) { //transactional
                if (s.getTransaction()==null) { s.createTransaction(0,null); }
                final EntityContainer to = (EntityContainer)this.sc.getConstructor(cs).newInstance(params);
                to.setTran(s.getTransaction());
                ident(to, s, null);
                return to;
            }
        }

        return null;
    }

    //used in DataChunk.getEntity
    public Object getInstance() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        final Class c = this.getTableClass();
        final SystemEntity ca = (SystemEntity)c.getAnnotation(SystemEntity.class);
        final IndexEntity xa = (IndexEntity)c.getAnnotation(IndexEntity.class);
        final ResultSetEntity ra = (ResultSetEntity)c.getAnnotation(ResultSetEntity.class);
        if (ca!=null||xa!=null||ra!=null) { //System non-transactional
            //Object o = c.getConstructor().newInstance();
            return c.getConstructor().newInstance();
            //only for user-defined entities
        } else {
            if (this.sc!=null) {
                return this.sc.getConstructor().newInstance();
            }
        }

        return null;
    }

    public IndexDescript[] getIndexNames() throws ClassNotFoundException, InternalException, MalformedURLException {
        final Class c = this.getTableClass();
        final Annotation[] ca = c.getAnnotations();
        final ArrayList<IndexDescript> r = new ArrayList<IndexDescript>();
        for (int i=0; i<ca.length; i++) {
            final Annotation a = ca[i];
            //for old javax.persistence versions
            if (a.annotationType().getName().equals("javax.persistence.Index")) {
                final Index ix = (Index)a;
                r.add(new IndexDescript(this, this.simpleName + "$" + ix.name(), ix.columnList(), ix.unique()));
            }
            if (a.annotationType().getName().equals("javax.persistence.Table")) {
                final javax.persistence.Table ta = (javax.persistence.Table) a;
                for (Index ix : ta.indexes()) {
                    r.add(new IndexDescript(this, this.simpleName + "$" + ix.name(), ix.columnList(), ix.unique()));
                }
            }
        }
        return r.toArray(new IndexDescript[]{});
    }

    public IndexDescript getIndexDescriptByColumnName(String name) throws ClassNotFoundException, InternalException, MalformedURLException {
        final Class c = this.getTableClass();
        final Annotation[] ca = c.getAnnotations();
        for (int i=0; i<ca.length; i++) {
            final Annotation a = ca[i];
            //for old javax.persistence versions
            if (a.annotationType().getName().equals("javax.persistence.Index")) {
                final Index ix = (Index)a;
                final IndexDescript id = new IndexDescript(this, this.simpleName + "$" + ix.name(), ix.columnList(), ix.unique());
                if (id.getColumns()[0].equals(name)) {
                    return id;
                }
            }
            if (a.annotationType().getName().equals("javax.persistence.Table")) {
                final javax.persistence.Table ta = (javax.persistence.Table) a;
                for (Index ix : ta.indexes()) {
                    final IndexDescript id = new IndexDescript(this, this.simpleName + "$" + ix.name(), ix.columnList(), ix.unique());
                    if (id.getColumns()[0].equals(name)) {
                        return id;
                    }
                }
            }
        }
        return null;
    }

    public Table getFirstIndexByColumnName(String name) throws ClassNotFoundException, InternalException, MalformedURLException {
        final Class c = this.getTableClass();
        final Annotation[] ca = c.getAnnotations();
        for (int i=0; i<ca.length; i++) {
            final Annotation a = ca[i];
            //for old javax.persistence versions
            if (a.annotationType().getName().equals("javax.persistence.Index")) {
                final Index ix = (Index)a;
                final IndexDescript id = new IndexDescript(this, ix.name(), ix.columnList(), ix.unique());
                if (id.getColumns()[0].equals(name)) {
                    return Instance.getInstance().getTableByName(SYSTEM_PKG_PREFIX + this.simpleName + "$" + ix.name());
                }
            }
            if (a.annotationType().getName().equals("javax.persistence.Table")) {
                final javax.persistence.Table ta = (javax.persistence.Table) a;
                for (Index ix : ta.indexes()) {
                    final IndexDescript id = new IndexDescript(this, ix.name(), ix.columnList(), ix.unique());
                    if (id.getColumns()[0].equals(name)) {
                        return Instance.getInstance().getTableByName(SYSTEM_PKG_PREFIX + this.simpleName + "$" + ix.name());
                    }
                }
            }
        }
        return null;
    }

    private Table getFirstIndexByIdColumn() throws ClassNotFoundException, InternalException, MalformedURLException {
        final Class c = this.getTableClass();
        final Annotation[] ca = c.getAnnotations();
        for (int i=0; i<ca.length; i++) {
            Annotation a = ca[i];
            //for old javax.persistence versions
            if (a.annotationType().getName().equals("javax.persistence.Index")) {
                final Index ix = (Index)a;
                final IndexDescript id = new IndexDescript(this, ix.name(), ix.columnList(), ix.unique());
                if (id.getColumns()[0].equals(getIdField().getName())) {
                    return Instance.getInstance().getTableByName(SYSTEM_PKG_PREFIX + this.simpleName + "$" + ix.name());
                }
            }
            if (a.annotationType().getName().equals("javax.persistence.Table")) {
                final javax.persistence.Table ta = (javax.persistence.Table) a;
                for (Index ix : ta.indexes()) {
                    final IndexDescript id = new IndexDescript(this, ix.name(), ix.columnList(), ix.unique());
                    if (id.getColumns()[0].equals(getIdField().getName())) {
                        return Instance.getInstance().getTableByName(SYSTEM_PKG_PREFIX + this.simpleName + "$" + ix.name());
                    }
                }
            }
        }
        return null;
    }

    private java.lang.reflect.Field[] getTableFields() throws ClassNotFoundException, MalformedURLException {
        final Class c = this.getTableClass();
        final ArrayList<java.lang.reflect.Field> res = new ArrayList<java.lang.reflect.Field>();
        final java.lang.reflect.Field[] f = c.getDeclaredFields();
        for (int i=0; i<f.length; i++) {
            Transient ta = f[i].getAnnotation(Transient.class);
            if (ta==null) {
                final int m = f[i].getModifiers();
                if (Modifier.isPrivate(m)) {
                    f[i].setAccessible(true);
                }
                res.add(f[i]);
            }
        }
        return res.toArray(new java.lang.reflect.Field[]{});
    }

    private String[] getTableFieldTypes() throws ClassNotFoundException, MalformedURLException {
        final java.lang.reflect.Field[] fs = getFields();
        String[] res = new String[fs.length];
        for (int i = 0; i < fs.length; i++) {
            res[i] = fs[i].getType().getName();
        }
        return res;
    }

    public java.lang.reflect.Field[] getFields() {
        return fields;
    }

    private java.lang.reflect.Field getTableIdField() throws ClassNotFoundException, MalformedURLException {
        final Class c = this.getTableClass();
        final java.lang.reflect.Field[] f = c.getDeclaredFields();
        for (int i=0; i<f.length; i++) {
            Id a = f[i].getAnnotation(Id.class);
            if (a != null) {
                return f[i];
            }
        }
        return null;
    }

    public boolean isIdFieldNoCheck() throws ClassNotFoundException, MalformedURLException {
        java.lang.reflect.Field f = getTableIdField();
        if (f != null) {
            NoCheck a = f.getAnnotation(NoCheck.class);
            return a != null;
        }
        return true;
    }

    public java.lang.reflect.Field getIdField() {
        return this.idfield;
    }

    public Method getIdmethod() {
        return idmethod;
    }

    public String getIdFieldType() {
        return this.idfieldtype;
    }

    public String getIdFieldGetter() {
        return this.idfieldgetter;
    }

    private java.lang.reflect.Field getGeneratedField() throws ClassNotFoundException, MalformedURLException {
        final Class c = this.getTableClass();
        final java.lang.reflect.Field[] f = c.getDeclaredFields();
        for (int i=0; i<f.length; i++) {
            GeneratedValue a = f[i].getAnnotation(GeneratedValue.class);
            if (a!=null) {
                return f[i];
            }
        }
        return null;
    }

    public Class<?> getTableClass() {
        //todo dirty hack for use sc class of sqlcursors (system, non-transact entities) & index entities
        //todo possibly deprecated
        if (sc!=null) {
            TransEntity sa = (TransEntity)sc.getAnnotation(TransEntity.class);
            if (sa==null) {
                return sc;
            }
        }
        return this.genericClass;
    }

    public void deallocate(Session s) throws Exception {
        final LinkedBlockingQueue<FrameData> frames = Instance.getInstance().getTableById(this.getObjectId()).getFrames();
        s.delete(this);
        boolean cnue = true;
        while (cnue) {
            final FrameData bd = frames.take();
            if (bd.getObjectId() == 0 && bd.getFrameId() == 0) {
                cnue = false;
            } else {
                //unsafe (chain-destruct) deallocate frame
                final FreeFrame fb = new FreeFrame(0, bd.getFrameId(), bd.getSize());
                fb.setPassed(1);
                final LLT llt = LLT.getLLT();
                try {
                    s.delete(bd, llt);
                    s.persist(fb, llt);
                } finally {
                    llt.commit();
                }
                bd.setFrame(null);
            }
        }
    }

    //constructor for SystemData - SystemFrame only
    public Table(String name) throws ClassNotFoundException, NoSuchMethodException, SecurityException, MalformedURLException {
        this.setName(name);
        this.setFrameSize(DataFile.SYSTEM_FRAME_SIZE);
        this.lbs = new WaitFrame[Config.getConfig().FILES_AMOUNT];
        this.genericClass = Class.forName(name);
        this.simpleName = genericClass.getSimpleName();
        SystemEntity ca = (SystemEntity)this.genericClass.getAnnotation(SystemEntity.class);
        IndexEntity xa = (IndexEntity)this.genericClass.getAnnotation(IndexEntity.class);
        this.notran = ca!=null;
        this.index = xa!=null;
        this.idfield = getTableIdField();
        this.idfieldtype = getTableIdField() == null ? null : getTableIdField().getType().getName();
        this.idfieldgetter = getTableIdField() == null ? null : ("get" + this.idfield.getName().substring(0, 1).toUpperCase() + this.idfield.getName().substring(1, this.idfield.getName().length()));
        this.idmethod = getTableIdField() == null ? null : ca == null ? null : getTableClass().getMethod(idfieldgetter, null);
        this.fields = getTableFields();
        this.fieldtypes = getTableFieldTypes();
        this.generatedfield = getGeneratedField();
    }

    public Table(String name, Class pclass) throws ClassNotFoundException, NoSuchMethodException, SecurityException, MalformedURLException {
        this.setName(name);
        this.lbs = new WaitFrame[Config.getConfig().FILES_AMOUNT];
        for (int i=0; i<Config.getConfig().FILES_AMOUNT; i++) {
            this.lbs[i] = new WaitFrame(null);
        }
        this.indexes = new ArrayList<IndexField>();
        this.maps = new ArrayList<MapField>();
        try {
            this.genericClass = Class.forName(name);
        } catch (ClassNotFoundException e) {
            this.genericClass = Instance.getUCL().loadClass(name);
        }
        this.simpleName = genericClass.getSimpleName();
        SystemEntity ca = (SystemEntity)this.genericClass.getAnnotation(SystemEntity.class);
        IndexEntity xa = (IndexEntity)this.genericClass.getAnnotation(IndexEntity.class);
        this.notran = ca!=null;
        this.index = xa!=null;
        this.idfield = getTableIdField();
        this.idfieldtype = getTableIdField() == null ? null : getTableIdField().getType().getName();
        this.idfieldgetter = getTableIdField() == null ? null : ("get" + this.idfield.getName().substring(0, 1).toUpperCase() + this.idfield.getName().substring(1, this.idfield.getName().length()));
        this.idmethod = getTableIdField() == null ? null : ca == null ? null : getTableClass().getMethod(idfieldgetter, null);
        this.fields = getTableFields();
        this.fieldtypes = getTableFieldTypes();
        this.generatedfield = getGeneratedField();
        this.sc = pclass;
    }

    public Table(String name, String name_) throws ClassNotFoundException, NoSuchMethodException, SecurityException, MalformedURLException {
        this.setName(name);
        this.lbs = new WaitFrame[Config.getConfig().FILES_AMOUNT];
        for (int i=0; i<Config.getConfig().FILES_AMOUNT; i++) {
            this.lbs[i] = new WaitFrame(null);
        }
        this.indexes = new ArrayList<IndexField>();
        this.maps = new ArrayList<MapField>();
        try {
            this.genericClass = Class.forName(name);
        } catch (ClassNotFoundException e) {
            this.genericClass = Instance.getUCL().loadClass(name);
        }
        this.simpleName = genericClass.getSimpleName();
        SystemEntity ca = (SystemEntity)this.genericClass.getAnnotation(SystemEntity.class);
        IndexEntity xa = (IndexEntity)this.genericClass.getAnnotation(IndexEntity.class);
        this.notran = ca!=null;
        this.index = xa!=null;
        this.idfield = getTableIdField();
        this.idfieldtype = getTableIdField() == null ? null : getTableIdField().getType().getName();
        this.idfieldgetter = getTableIdField() == null ? null : ("get" + this.idfield.getName().substring(0, 1).toUpperCase() + this.idfield.getName().substring(1, this.idfield.getName().length()));
        this.idmethod = getTableIdField() == null ? null : ca == null ? null : getTableClass().getMethod(idfieldgetter, null);
        this.fields = getTableFields();
        this.fieldtypes = getTableFieldTypes();
        this.generatedfield = getGeneratedField();
    }

    //used in bootstrap - only for FrameData
    public Table(boolean v) throws ClassNotFoundException, InstantiationException, InternalException, IllegalAccessException, NoSuchMethodException, SecurityException, MalformedURLException {
        this.objectId = FrameData.CLASS_ID;
        this.name = FrameData.class.getName();
        this.frameSize = Instance.getInstance().getFrameSize();
        this.lbs = new WaitFrame[Config.getConfig().FILES_AMOUNT];
        for (int i=0; i<Config.getConfig().FILES_AMOUNT; i++) {
            this.lbs[i] = new WaitFrame(null);
        }
        this.indexes = new ArrayList<IndexField>();
        this.maps = new ArrayList<MapField>();
        this.genericClass = Class.forName(name);
        this.simpleName = genericClass.getSimpleName();
        SystemEntity ca = (SystemEntity)this.genericClass.getAnnotation(SystemEntity.class);
        IndexEntity xa = (IndexEntity)this.genericClass.getAnnotation(IndexEntity.class);
        this.notran = ca!=null;
        this.index = xa!=null;
        this.idfield = getTableIdField();
        this.idfieldtype = getTableIdField() == null ? null : getTableIdField().getType().getName();
        this.idfieldgetter = getTableIdField() == null ? null : ("get" + this.idfield.getName().substring(0, 1).toUpperCase() + this.idfield.getName().substring(1, this.idfield.getName().length()));
        this.idmethod = getTableIdField() == null ? null : ca == null ? null : getTableClass().getMethod(idfieldgetter, null);
        this.fields = getTableFields();
        this.fieldtypes = getTableFieldTypes();
        this.generatedfield = getGeneratedField();
    }

    //constructor for inital system method
    public Table(int id, String name) throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, SecurityException {
        this.objectId = id;
        this.name = name;
        this.indexes  = new ArrayList<IndexField>();
        this.maps = new ArrayList<MapField>();
        this.lbs = new WaitFrame[Config.getConfig().FILES_AMOUNT];
        this.genericClass = Class.forName(name);
        this.simpleName = genericClass.getSimpleName();
        SystemEntity ca = (SystemEntity)this.genericClass.getAnnotation(SystemEntity.class);
        IndexEntity xa = (IndexEntity)this.genericClass.getAnnotation(IndexEntity.class);
        this.notran = ca!=null;
        this.index = xa!=null;
        this.idfield = getTableIdField();
        this.idfieldtype = getTableIdField() == null ? null : getTableIdField().getType().getName();
        this.idfieldgetter = getTableIdField() == null ? null : ("get" + this.idfield.getName().substring(0, 1).toUpperCase() + this.idfield.getName().substring(1, this.idfield.getName().length()));
        this.idmethod = getTableIdField() == null ? null : ca == null ? null : getTableClass().getMethod(idfieldgetter, null);
        this.generatedfield = getGeneratedField();
        this.fields = getTableFields();
        this.fieldtypes = getTableFieldTypes();
        initIndexFields();
    }

    //constructor for low-level storage function (initial first-time load table descriptions from datafile)
    public Table (DataChunk chunk, IndexList ixl) throws IllegalAccessException, ClassNotFoundException, IOException, InternalException, NoSuchMethodException, SecurityException {
        final Object[] dcs = chunk.getDcs().getValueSet();
        final Class c = this.getClass();
        final java.lang.reflect.Field[] f = c.getDeclaredFields();
        int x = 0;
        for (int i=0; i<f.length; i++) {
            final Transient ta = f[i].getAnnotation(Transient.class);
            if (ta==null) {
                int m = f[i].getModifiers();
                if (Modifier.isPrivate(m)) {
                    f[i].setAccessible(true);
                }
                f[i].set(this, dcs[x]);
                x++;
            }
        }
        //this.genericClass = Class.forName(name);
        this.genericClass = Instance.getUCL().loadClass(name);
        this.simpleName = genericClass.getSimpleName();
        final SystemEntity sa = (SystemEntity)this.genericClass.getAnnotation(SystemEntity.class);
        final IndexEntity xa = (IndexEntity)this.genericClass.getAnnotation(IndexEntity.class);
        final ResultSetEntity rsa = (ResultSetEntity)this.genericClass.getAnnotation(ResultSetEntity.class);
        this.notran = sa!=null;
        this.index = xa!=null;
        this.idfield = getTableIdField();
        this.idfieldtype = getTableIdField() == null ? null : getTableIdField().getType().getName();
        this.idfieldgetter = getTableIdField() == null ? null : ("get" + this.idfield.getName().substring(0, 1).toUpperCase() + this.idfield.getName().substring(1, this.idfield.getName().length()));
        this.idmethod = getTableIdField() == null ? null : sa == null ? null : getTableClass().getMethod(idfieldgetter, null);
        this.fields = getTableFields();
        this.fieldtypes = getTableFieldTypes();
        this.generatedfield = getGeneratedField();
        this.indexes = new ArrayList<IndexField>();
        this.maps = new ArrayList<MapField>();

        initIndexFields();

        if (this.name.equals(FrameData.class.getName())) {
            getIndexFieldByColumn("objectId").setIndex(ixl);
            final Map<Long, Object> ixlb = new ConcurrentHashMap<>();
            final Map<Long, Object> ixla = new ConcurrentHashMap<>();
            final IndexList ixls = new IndexList();
            for (Object o : ixl.getContent()) {
                ixlb.put(((FrameData) ((DataChunk) o).getEntity()).getFrameId(), o);
                ixla.put(((FrameData) ((DataChunk) o).getEntity()).getAllocId(), o);
                ixls.add(((FrameData) ((DataChunk) o).getEntity()).getStarted(), o);
            }
            getMapFieldByColumn("frameId").setMap(ixlb);
            getMapFieldByColumn("allocId").setMap(ixla);
        } else if (this.name.equals(UndoChunk.class.getName())) {
            //none
        } else {
            try {
                final List<Object> bds = ixl.getObjectsByKey(this.getObjectId());
                for (Object b : bds) {
                    final FrameData bd = (FrameData)((DataChunk)b).getEntity();
                    bd.setDataObject(this); //todo must be refactored, FrameData->new DataFrame->new DataChunk->t.getFields() possibly may be simply

                    if (sa != null && rsa == null && this.objectId != Table.CLASS_ID) {
                        Frame db = null;
                        try {
                            db = bd.getFrame();
                        } catch (Exception e) {
                            logger.error("internal Table.<init>");
                        }
                        for (Chunk ck : ((DataFrame)db).getChunks()) {
                            if (ck.getHeader().getState() == Header.RECORD_NORMAL_STATE) {  //miss deleted or archived records
                                this.addIndexValue((DataChunk) ck);
                            }
                        }
                    }
                    //set start frames for remote indexes
                    if (bd.getStarted() > 0) {
                        ixstartfs.put(bd.getStarted(), bd.getFrameId());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //get LBS frames
        if (!this.isIndex()&&!this.name.equals(UndoChunk.class.getName())) {
            final List<Object> bds = ixl.getObjectsByKey(this.objectId);
            final ArrayList<WaitFrame> lbs = new ArrayList<WaitFrame>();
            for (Object b : bds) {
                FrameData bd = (FrameData) ((DataChunk) b).getEntity();
                if (bd.getCurrent() != null && bd.getCurrent().get() > 0) {
                    lbs.add(new WaitFrame(bd));
                }
            }
            if (!(lbs.size() == 1 || lbs.size() == Config.getConfig().FILES_AMOUNT)) { //paranoid check
                if (lbs.size() == 0 && this.name.equals(UndoChunk.class.getName())) {
                    //DataChunk not used pre-allocated frames
                } else {
                    for (WaitFrame wb : lbs) {
                        logger.error("set lb during init table: " + wb.getBd().getObjectId() + ":" + wb.getBd().getFile() + ":" + wb.getBd().getPtr());
                    }
                    throw new InternalException();
                }
            }
            this.lbs = lbs.toArray(new WaitFrame[]{});
        } else {
            this.lbs = new WaitFrame[]{};
        }

    }

    public void initIndexFields () throws ClassNotFoundException, MalformedURLException {
        final Class c = this.getTableClass();
        final java.lang.reflect.Field[] f = c.getDeclaredFields();
        for (int i=0; i<f.length; i++) {
            final IndexColumn a = f[i].getAnnotation(IndexColumn.class);
            final MapColumn b = f[i].getAnnotation(MapColumn.class);
            if (a!=null) {
                indexes.add(new IndexField(f[i], new IndexList()));
            }
            if (b!=null) {
                maps.add(new MapField(f[i], new ConcurrentHashMap()));
            }
        }
    }

    public void addIndexValue (DataChunk dc) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        final Object o = dc.getEntity();
        final Class c = o.getClass();
        final TransEntity ta = (TransEntity)c.getAnnotation(TransEntity.class);
        if (ta==null) {
            for (IndexField ix : indexes) {
                final Method z = c.getMethod("get" + ix.getField().getName().substring(0, 1).toUpperCase() + ix.getField().getName().substring(1, ix.getField().getName().length()), null);
                final Object v = z.invoke(o, null);
                ix.getIndex().add(new IndexElementKey(new Object[]{v}), dc);
            }
            for (MapField map : maps) {
                final Method z = c.getMethod("get" + map.getField().getName().substring(0, 1).toUpperCase() + map.getField().getName().substring(1, map.getField().getName().length()), null);
                final Object v = z.invoke(o, null);
                map.getMap().put(v, dc);
            }
        }
    }

    private void updateIndexValue(DataChunk dc) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        final Object o = dc.getEntity();
        final Class c = o.getClass();
        final TransEntity ta = (TransEntity) c.getAnnotation(TransEntity.class);
        if (ta == null) {
            for (IndexField ix : indexes) {
                final Method z = c.getMethod("get" + ix.getField().getName().substring(0, 1).toUpperCase() + ix.getField().getName().substring(1, ix.getField().getName().length()), null);
                final Object v = z.invoke(o, null);
                ix.getIndex().update(new IndexElementKey(new Object[]{v}), dc);
            }
            for (MapField map : maps) {
                final Method z = c.getMethod("get" + map.getField().getName().substring(0, 1).toUpperCase() + map.getField().getName().substring(1, map.getField().getName().length()), null);
                final Object v = z.invoke(o, null);
                map.getMap().put(v, dc);
            }
        }
    }

    private void removeIndexValue(DataChunk dc) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        final Object o = dc.getEntity();
        final Class c = o.getClass();
        final TransEntity ta = (TransEntity)c.getAnnotation(TransEntity.class);
        if (ta==null) {
            for (IndexField ix : indexes) {
                final Method z = c.getMethod("get" + ix.getField().getName().substring(0, 1).toUpperCase() + ix.getField().getName().substring(1, ix.getField().getName().length()), null);
                final Object v = z.invoke(o, null);
                ix.getIndex().remove(new IndexElementKey(new Object[]{v}), dc);
            }
            for (MapField map : maps) {
                final Method z = c.getMethod("get" + map.getField().getName().substring(0, 1).toUpperCase() + map.getField().getName().substring(1, map.getField().getName().length()), null);
                final Object v = z.invoke(o, null);
                map.getMap().remove(v);
            }
        }
    }

    @Deprecated
    public void updateIndexes(Frame b) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        for (Chunk c : b.getChunks()) {
            if (c.getHeader().getState() == Header.RECORD_NORMAL_STATE) {  //miss deleted or archived records
                //Object o = ((DataChunk)c).getEntity(this);
                updateIndexValue((DataChunk)c);
            }
        }
    }

    public IndexField getIndexFieldByColumn(String name) {
        for (IndexField ix : indexes) {
            if (ix.getField().getName().equals(name)) {
                return ix;
            }
        }
        return null;
    }

    public MapField getMapFieldByColumn(String name) {
        for (MapField map : maps) {
            if (map.getField().getName().equals(name)) {
                return map;
            }
        }
        return null;
    }

    public IndexField getIdIndexField() throws MalformedURLException, ClassNotFoundException {
        java.lang.reflect.Field id = this.getIdField();
        if (id!=null) {
            for (IndexField ix : indexes) {
                if (ix.getField().getName().equals(id.getName())) {
                    return ix;
                }
            }
        }
        return null;
    }

    public void printIndexInfo() {
        System.out.println("Index info for "+this.getName());
        if (this.sc!=null) {
            System.out.println("System sc: "+this.sc.getName());
        }
        for (MapField map : maps) {
            System.out.println("map: "+map.getField().getName()+" "+map.getMap().size());
        }
        for (IndexField ix : indexes) {
            System.out.println("index: "+ix.getField().getName()+" "+ix.getIndex().getInfo());
        }
    }

    private void ident(final Object o, final Session s, final LLT extllt) throws Exception {
        final Class c_ = o.getClass();
        final TransEntity ta = (TransEntity)c_.getAnnotation(TransEntity.class);
        //for Transactional Wrapper Entity we must get superclass (original Entity class)
        final Class c = ta != null ? o.getClass().getSuperclass() : o.getClass();
        final Entity ea = (Entity)c.getAnnotation(Entity.class);
        if (ea == null) {
            throw new InternalException();
        }
        final java.lang.reflect.Field[] f = c.getDeclaredFields();
        for (int i=0; i<f.length; i++) {
            final Column ca = f[i].getAnnotation(Column.class);
            final Transient tr = f[i].getAnnotation(Transient.class);
            final GeneratedValue ga = f[i].getAnnotation(GeneratedValue.class);
            final DistributedId ds = f[i].getAnnotation(DistributedId.class);
            if (tr==null) {
                if (ga!=null) {
                    int m = f[i].getModifiers();
                    if (Modifier.isPrivate(m)) {
                        f[i].setAccessible(true);
                    }
                    final LLT llt = extllt == null ? LLT.getLLT() : extllt;
                    try {
                        if (ds == null) {
                            if (f[i].getType().getName().equals("int")) {
                                f[i].setInt(o, (int) this.getIdValue(s, llt));
                            }
                            if (f[i].getType().getName().equals("long")) {
                                f[i].setLong(o, this.getIdValue(s, llt));
                            }
                        } else {
                            //for distributed ids, id value don't replace exists > 0
                            if (f[i].getType().getName().equals("int")) {
                                final int exists = (int) f[i].get(o);
                                if (exists == 0) {
                                    f[i].setInt(o, (int) (this.getIdValue(s, llt) * Storage.MAX_NODES) + Config.getConfig().LOCAL_NODE_ID);
                                }
                            }
                            if (f[i].getType().getName().equals("long")) {
                                final long exists = (long) f[i].get(o);
                                if (exists == 0) {
                                    f[i].setLong(o, (this.getIdValue(s, llt) * Storage.MAX_NODES) + Config.getConfig().LOCAL_NODE_ID);
                                }
                            }
                        }
                    } finally {
                        if (extllt == null) { llt.commit(); }
                    }
                    break;
                }
            }
        }
    }

    public void usedSpace (final FrameData bd, final int used, final boolean persist, final Session s, LLT llt) {
        bd.setUsed(used);
        if (used==0) {
            if (!checkLBS(bd)) {
                //deallocate frame
                final FreeFrame fb = new FreeFrame(0, bd.getFrameId(), bd.getSize());
                final FrameData pb = bd.getPrevFrameId()>0?Instance.getInstance().getFrameById(bd.getPrevFrameId()):null;
                final FrameData nb = Instance.getInstance().getFrameById(bd.getNextFrameId());
                nb.setPrevFile(pb==null?0:pb.getFile());
                nb.setPrevFrame(pb==null?0:pb.getPtr());
                try {
                    if (pb!=null) {
                        pb.setNextFile(nb.getFile());
                        pb.setNextFrame(nb.getPtr());
                        s.persist(pb, llt); //update
                    }
                    s.persist(nb, llt); //update
                    s.persist(fb, llt); //insert
                    s.delete(bd, llt);
                    this.frameAmount.decrementAndGet();
                    s.persist(this, llt);
                    logger.debug("deallocate frame " + bd.getObjectId() + ":" + bd.getFile() + ":" + bd.getPtr() + " " + Thread.currentThread().getName());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            try {
                if (persist) {
                    final Table t = Instance.getInstance().getTableByName(FrameData.class.getName());
                    final DataChunk dc = t.getChunkByEntity(bd, s);
                    final FrameData bd_ = Instance.getInstance().getFrameById(dc.getHeader().getRowID().getFileId() + dc.getHeader().getRowID().getFramePointer());
                    bd_.updateChunk(dc, bd, s, llt);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private WaitFrame getAvailableFrame(final Object o, final boolean fpart) throws ClassNotFoundException, InstantiationException, InternalException, IllegalAccessException {
        Metrics.get("getAvailableFrame").start();
        final long st = System.currentTimeMillis();

        //todo move to config
        final long timeout = 100;

        final int a = avframeStart.get()%this.lbs.length;
        while (true) {
            final long tp = System.currentTimeMillis() - st;
            for (int i = 0; i < this.lbs.length; i++) {
                final int i_ = (a + i) % this.lbs.length;
                final WaitFrame wb = this.lbs[i_];
                final WaitFrame bd = fpart ? wb.acquire(getTargetFileId(((FilePartitioned) o).getFile()), false) : wb.acquire(false);
                if (bd != null) {
                    avframeStart.getAndIncrement();
                    Metrics.get("getAvailableFrame").stop();
                    return bd;
                }
            }
            if (tp > timeout) {
                for (int i = 0; i < this.lbs.length; i++) {
                    logger.warn("lbs: "+lbs[i].getBd().getFrameId()+":"+lbs[i].getBusy().get());
                }
                logger.warn("avframestart: "+avframeStart.get());
                logger.warn("timeout occured during getavailableframe method: " + timeout);
                for (int i = 0; i < this.lbs.length; i++) {
                    final int i_ = (a + i) % this.lbs.length;
                    final WaitFrame wb = this.lbs[i_];
                    final WaitFrame bd = fpart ? wb.acquire(getTargetFileId(((FilePartitioned) o).getFile()), true) : wb.acquire(true);
                    if (bd != null) {
                        logger.warn("forced acquire: "+bd.getBd().getFrameId());
                        avframeStart.getAndIncrement();
                        Metrics.get("getAvailableFrame").stop();
                        return bd;
                    }
                }
                break;
            }
        }
        Metrics.get("getAvailableFrame").stop();
        return null;
    }

    private int getTargetFileId(final int fileId) throws ClassNotFoundException, InstantiationException, InternalException, IllegalAccessException {
        for (DataFile f : Storage.getStorage().getInitDataFiles()) {
            if (f.order(fileId)) {
                return f.getFileId();
            }
        }
        return 0;
    }

    //used for prevent the release of current frames
    //contains hard hack for index tables - always return true
    protected boolean checkLBS(FrameData bd) {
        if (this.index) {
            return true;
        }
        for (WaitFrame wb : this.lbs) {
            if (wb.getBd().getFrameId()==bd.getFrameId()) {
                return true;
            }
        }
        return false;
    }

    public synchronized DataChunk persist (final Object o, final Session s) throws Exception {
        return persist(o, s, null);
    }

    protected DataChunk persist (final Object o, final Session s, final LLT extllt) throws Exception {
        final Class cc = o.getClass();
        boolean fpart = false;

        if (this.isNoTran()) {
            for (Class i : cc.getInterfaces()) {
                if (i.getName().equals(FilePartitioned.class.getName())) {
                    fpart = true;
                }
            }
        } else {
            if (s.getTransaction() == null || !s.getTransaction().started || s.getTransaction().getMTran() == 0) {
                s.startStatement();
            }
            if (s.getTransaction() == null || !s.getTransaction().started || s.getTransaction().getMTran() == 0) {
                throw new InternalException();
            }
            final EntityContainer to = (EntityContainer)o;
            if (to.getTran() != null && to.getTran().getCid() == 0) {
                if (to.getTran().getTransId() != s.getTransaction().getTransId()) {
                    logger.error("unable to persist an object that has not been changed by current transaction");
                    return null;
                }
            }
        }

        final LLT llt = extllt==null?LLT.getLLT():extllt;
        try {
            if (this.isIndex()) {
                this.add(new RowId(0, 0, 0), o, s, llt);
                return null;
            }

            Metrics.get("persistGetChunk").start();
            final DataChunk dc = isIdFieldNoCheck() ? null : this.getChunkByEntity(o, s);
            Metrics.get("persistGetChunk").stop();

            if (dc == null) {
                Metrics.get("persistInsertChunk").start();
                if (this.isNoTran()) {
                    this.ident(o, s, llt); //ident system entities during persist
                }

                final DataChunk nc = new DataChunk(o, s);
                final int len = nc.getBytesAmount();
                final WaitFrame bdw = getAvailableFrame(o, fpart);
                final FrameData bd = bdw.getBd();
                final int diff = len - bd.getFrameFree();

                if (isNoTran()) {
                    final int p = bd.insertChunk(nc, s, true, llt);
                    if (p == 0) {
                        final FrameData nb = this.createNewFrame(bd, bd.getFile(), 0, 0, false, true, false, s, llt);
                        nb.getDataFrame().insertChunk(nc, s, true, llt);
                        usedSpace(nb, nb.getUsed() + len, true, s, llt);
                    } else {
                        usedSpace(bd, bd.getUsed() + len, true, s, llt);
                    }
                } else {
                    if (diff > 0) {
                        final FrameData nb = this.createNewFrame(bd, bd.getFile(), 0, 0, false, true, false, s, llt);
                        nb.getDataFrame().insertChunk(nc, s, true, llt);
                        s.getTransaction().storeFrame(nb, len, s, llt);
                    } else {
                        final int p = bd.insertChunk(nc, s, true, llt);
                        if (p == 0) {
                            throw new InternalException();
                        }
                        s.getTransaction().storeFrame(bd, len, s, llt);
                    }
                    ((EntityContainer) o).setTran(nc.getHeader().getTran());
                    ((EntityContainer) o).setRowId(nc.getHeader().getRowID());
                    ((EntityContainer) o).setDataChunk(nc);
                }

                //system-only table in-memory indexes
                this.addIndexValue(nc);
                bdw.release();
                Metrics.get("persistInsertChunk").stop();

                Metrics.get("persistInsertIndex").start();
                //remove external llt for deadlock prevent
                persistIndexes(nc, s, llt);
                Metrics.get("persistInsertIndex").stop();

                return nc;

            } else {

                DataChunk udc = null;

                if (!isNoTran()) { //save undo information
                    udc = dc.lock(s, llt);
                }

                final int len = dc.getBytesAmount();
                final FrameData bd = Instance.getInstance().getFrameById(dc.getHeader().getRowID().getFileId() + dc.getHeader().getRowID().getFramePointer());
                final int newlen = bd.updateChunk(dc, o, s, llt);
                final int diff = newlen - len - bd.getFrameFree();

                if (diff > 0) {
                    lockIndexes(dc, s, llt);
                    bd.removeChunk(dc.getHeader().getRowID().getRowPointer(), s, llt);
                    final WaitFrame ibw = getAvailableFrame(o, fpart);
                    final FrameData ib = ibw.getBd();

                    final int p = ib.getDataFrame().insertChunk(dc, s, true, llt);
                    if (p == 0) {
                        final FrameData nb = this.createNewFrame(ib, ib.getFile(), 0, 0, false, true, false, s, llt);
                        nb.getDataFrame().insertChunk(dc, s, true, llt);
                        if (isNoTran()) {
                            usedSpace(bd, bd.getUsed() - len, true, s, llt);
                            usedSpace(nb, newlen, true, s, llt);
                        } else {
                            s.getTransaction().storeFrame(bd, udc == null ? null : udc.getUframe(),0 - len, s, llt);
                            s.getTransaction().storeFrame(nb, udc == null ? null : udc.getUframe(), newlen, s, llt);
                            s.getTransaction().storeFrame(nb, newlen, s, llt);
                        }
                    } else {
                        if (isNoTran()) {
                            usedSpace(bd, bd.getUsed() - len, true, s, llt);
                            usedSpace(ib, ib.getUsed() + newlen, true, s, llt);
                        } else {
                            s.getTransaction().storeFrame(bd, udc == null ? null : udc.getUframe(),0 - len, s, llt);
                            s.getTransaction().storeFrame(ib, udc == null ? null : udc.getUframe(), newlen, s, llt);
                            s.getTransaction().storeFrame(ib, newlen, s, llt);
                        }
                    }
                    updateIndexesPtr(dc, s, llt);
                    //update rowid
                    if (!isNoTran()) {
//                        logger.debug("updated: " + dc.getHeader().getRowID() + ", old: "+dc.getUndoChunk().getFile()+" "+dc.getUndoChunk().getFrame()+" "+dc.getUndoChunk().getPtr());
                        ((EntityContainer) o).setRowId(dc.getHeader().getRowID());
                        dc.getUndoChunk().setFile(dc.getHeader().getRowID().getFileId());
                        dc.getUndoChunk().setFrame(dc.getHeader().getRowID().getFramePointer());
                        dc.getUndoChunk().setPtr(dc.getHeader().getPtr());
                        if (udc != null) {
                            udc.setEntity(dc.getUndoChunk());
                            udc.getUframe().updateChunk(udc, dc.getUndoChunk(), s, llt);
                        }
                    }

                    ibw.release();
                    if (extllt == null) {
                        llt.commit();
                    }

                    return dc;

                } else {
                    if (isNoTran()) {
                        usedSpace(bd, bd.getUsed() + newlen - len, true, s, llt);
                    } else {
                        s.getTransaction().storeFrame(bd, udc == null ? null : udc.getUframe(), newlen - len, s, llt);
                    }
                }

                return dc;
            }
        } finally {
            if (extllt == null) {
                llt.commit();
            }
        }
    }

    private void persistIndexes(DataChunk c, Session s, LLT llt) throws Exception {
        for (IndexDescript ids : this.getIndexNames()) {
            final Table ixt = Instance.getInstance().getTableByName(SYSTEM_PKG_PREFIX + ids.getName());
            //create IndexChunk implementation
            final Object io = ixt.getTableClass().getConstructor(new Class<?>[]{c.getClass(),s.getClass()}).newInstance(new Object[]{c, s});
            ixt.add(c.getHeader().getRowID(), io, s, llt);
        }
    }

    protected void delete (final Object o, final Session s, LLT extllt, boolean ignoreTransaction) throws Exception {
        final boolean noTran = ignoreTransaction ? true : isNoTran();
        if (!noTran) {
            if (s.getTransaction() == null || !s.getTransaction().started || s.getTransaction().getMTran() == 0) {
                s.startStatement();
            }
            if (s.getTransaction() == null || !s.getTransaction().started || s.getTransaction().getMTran() == 0) {
                throw new InternalException();
            }
        }

        final DataChunk dc = this.getChunkByEntity(o, s);
        if (dc == null) {
            throw new CannotAccessToDeletedRecord();
        }
        if (dc.getHeader().getState() == Header.RECORD_DELETED_STATE) {
            throw new CannotAccessToDeletedRecord();
        }
        final int len = dc.getBytesAmount();
        final FrameData bd = Instance.getInstance().getFrameById(dc.getHeader().getRowID().getFileId()+dc.getHeader().getRowID().getFramePointer());
        final LLT llt = extllt==null?LLT.getLLT():extllt;

        DataChunk udc = null;

        try {
            if (!noTran) { //save undo information
                udc = dc.lock(s, llt);
            }

            if (bd == null) {
                logger.error("cannot found frame " + dc.getHeader().getRowID().getFileId() + ":" + dc.getHeader().getRowID().getFramePointer() + " during delete " + o.getClass().getSimpleName() + Thread.currentThread().getName());
            }
            if (ignoreTransaction || udc == null) {
                deleteIndexes(dc, noTran, true, s, llt);
                bd.removeChunk(dc.getHeader().getRowID().getRowPointer(), s, llt);
            } else {
                deleteIndexes(dc, noTran, false, s, llt);
                bd.deleteChunk(dc.getHeader().getRowID().getRowPointer(), s, llt);
            }
            removeIndexValue(dc);

            if (noTran) {
                usedSpace(bd, bd.getUsed() - len, true, s, llt);
            } else {
                s.getTransaction().storeFrame(bd, udc == null ? null : udc.getUframe(), 0 - len, s, llt);
            }
        } finally {
            if (extllt == null) { llt.commit(); }
        }
    }

    private void deleteIndexes(DataChunk dc, boolean noTran, boolean remove, Session s, LLT llt) throws Exception {
        for (IndexDescript ids : this.getIndexNames()) {
            final DataChunk ic = dc.getIc(ids, s);
            final int iclen = ic.getBytesAmount();
            final FrameData ibd = Instance.getInstance().getFrameById(ic.getHeader().getRowID().getFileId() + ic.getHeader().getRowID().getFramePointer());
            final DataChunk udc = remove ? null : ic.lock(s, llt);
            if (remove) {
                ibd.removeChunk(ic.getHeader().getRowID().getRowPointer(), s, llt);
            } else {
                ibd.deleteChunk(ic.getHeader().getRowID().getRowPointer(), s, llt);
            }
            if (noTran) {
                usedSpace(ibd, ibd.getUsed() - iclen, true, s, llt);
            } else {
                s.getTransaction().storeFrame(ibd, udc == null ? null : udc.getUframe(),0 - iclen, s, llt);
            }
        }
        if (remove) {
            dc.clearIcs();
        }
    }

    private void lockIndexes(DataChunk dc, Session s, LLT llt) throws Exception {
        for (IndexDescript ids : this.getIndexNames()) {
            final DataChunk ic = dc.getIc(ids, s);
            final int iclen = ic.getBytesAmount();
            final FrameData ibd = Instance.getInstance().getFrameById(ic.getHeader().getRowID().getFileId() + ic.getHeader().getRowID().getFramePointer());
            final DataChunk udc = ic.lock(s, llt);
            s.getTransaction().storeFrame(ibd, udc == null ? null : udc.getUframe(),0 - iclen, s, llt);
        }
    }

    private void updateIndexesPtr(DataChunk dc, Session s, LLT llt) throws Exception {
        for (IndexDescript ids : this.getIndexNames()) {
            final DataChunk ic = dc.getIc(ids, s);
            final FrameData xf = Instance.getInstance().getFrameById(ic.getHeader().getRowID().getFileId() + ic.getHeader().getRowID().getFramePointer());
            llt.add(xf.getFrame());
            final IndexChunk ic_ = (IndexChunk) ic.getEntity();
            ic_.setFramePtrRowId(dc.getHeader().getRowID());
            ic_.setDataChunk(dc);
            ic.getHeader().setFramePtr(dc.getHeader().getRowID());
        }
    }

    //todo deprecated started param
    public FrameData createNewFrame(final FrameData frame, final int fileId, final int frameType, final long allocId, final boolean started, final boolean setlbs, final boolean external, final Session s, final LLT llt) throws Exception {
        final DataFile df = Storage.getStorage().getDataFileById(fileId);
        final FrameData bd = df.createNewFrame(frame, frameType, allocId, started, external, this, s, llt);
        if (!external) {
            synchronized (this) {
                boolean done = true;
//System.out.println("Table.createNewFrame: old="+(frame==null?"null":frame.getFrameId())+" frame="+(frame.getFrame()==null?"null":(frame.getFrame().getFrameData()==null?":null":frame.getFrame().getFrameData().getFrameId())));
                if (setlbs && !this.getName().equals(UndoChunk.class.getName())) {
                    done = false;
                    for (WaitFrame wb : this.lbs) {
                        if (wb.trySetBd(frame, bd, frameType)) {
                            done = true;
                            break;
                        }
                    }
                }
                if (!done) {
                    // todo evicted frame -> metric
                    for (WaitFrame wb : this.lbs) {
                        if (wb.getBd().getFile() == bd.getFile()) {
                            // remove evicted ptr from prevframe
                            frame.setNextFrame(wb.getBd().getPtr());
                            s.persist(frame, llt);
                        }
                    }
                    bd.clearCurrent();
                    s.persist(bd, llt);
                    logger.info("evict frame " + bd.getObjectId() + ":" + bd.getFile() + ":" + bd.getPtr() + " " + Thread.currentThread().getName());
                    for (WaitFrame wb : this.lbs) {
                        final WaitFrame bd_ = wb.acquire(fileId, false);
                        if (bd_ != null) {
                            return bd_.getBd();
                        }
                    }
                }
            }
        }
        return bd;
    }

    public synchronized FrameData allocateFrame(DataFile df, Table t, Session s, LLT llt) throws Exception {
        return df.allocateFrame(t, s, llt);
    }

    public void lockTable(Session s) {
        try {
            s.persist(new RetrieveLock(this.objectId, s.getTransaction().getTransId())); //insert
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Deprecated
    public void unlockTable(Session s) {
        try {
            final RetrieveLock rl = Instance.getInstance().getRetrieveLockById(this.objectId, s.getTransaction().getTransId());
            if (rl!=null) {
                final List<TransFrame> tbs = Instance.getInstance().getTransFrameByObjectId(this.objectId);
                final List<Long> ts = new ArrayList<Long>();
                for (TransFrame tb : tbs) {
                    if (!ts.contains(tb.getTransId())) {
                        ts.add(tb.getTransId());
                    }
                }
                for (long t : ts) {
                    final Transaction tr = Instance.getInstance().getTransactionById(t);
                    if (tr.getTransType()==Transaction.TRAN_THR) {
                        tr.unlockUndoFrames(this.objectId, s);
                    }
                }
                s.delete(rl);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected RetrieveQueue getContentQueue(Session s) {
        try {
            if (this.isIndex()) {
                return getIndexContentQueue(s);
            } else {
                return getTableContentQueue(s);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    protected synchronized RetrieveQueue getTableContentQueue(Session s) {
        final Table t = this;
        final LinkedBlockingQueue<Chunk> q = new LinkedBlockingQueue<>(Config.getConfig().RETRIEVE_QUEUE_SIZE);
        final AtomicBoolean stopped = new AtomicBoolean();
        final Chunk tc = new DataChunk();
        tc.setTerminate(true);
        final ManagedCallable<Boolean> r = new ManagedCallable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Thread.currentThread().setName("interference-retrieve-queue-thread");
                synchronized (t) {
                    final LinkedBlockingQueue<FrameData> bds = Instance.getInstance().getTableById(getObjectId()).getFrames();
                    boolean cnue = true;
                    while (cnue) {
                        final FrameData b = bds.take();
                        if (b.getObjectId() == 0 && b.getFrameId() == 0) {
                            cnue = false;
                        } else {
                            if (stopped.get()) {
                                return true;
                            }
                            for (Chunk c : b.getDataFrame().getFrameChunks(s)) {
                                q.put(c);
                            }
                        }
                    }
                }
                q.put(tc);
                return true;
            }

            @Override
            public void stop() {
                stopped.set(true);
            }
        };
        return new RetrieveQueue(q, r);
    }

    protected synchronized RetrieveQueue getIndexContentQueue(Session s) {
        final Table t = this;
        final int ptr = 0;
        final LinkedBlockingQueue<Chunk> q = new LinkedBlockingQueue<>(Config.getConfig().RETRIEVE_QUEUE_SIZE);
        final AtomicBoolean stopped = new AtomicBoolean();
        final Chunk tc = new DataChunk();
        tc.setTerminate(true);
        final ManagedCallable<Boolean> r = new ManagedCallable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                Thread.currentThread().setName("interference-retrieve-index-queue-thread");
                synchronized (t) {
                    List<Long> startframes = new ArrayList<>();
                    startframes.add(t.fileStart + t.frameStart);
                    for (Map.Entry<Integer, Long> entry : ixstartfs.entrySet()) {
                        startframes.add(entry.getValue());
                    }

                    //todo need to implement merge algorithm for multinode indexes
                    for (long start : startframes) {
                        List<IndexFrame> levelNodes = new ArrayList<>();
                        boolean cnue = true;
                        final FrameData bd = Instance.getInstance().getFrameById(start);
                        if (bd == null) {
                            throw new InternalException();
                        }
                        //frame must be local or remote chain started (RCS)
                        if (bd.getFrameId() != bd.getAllocId() && bd.getStarted() == 0) {
                            throw new InternalException();
                        }
                        IndexFrame el = bd.getIndexFrame();
                        el.sort();
                        levelNodes.add(el);
                        while (cnue) {
                            ArrayList<IndexFrame> inNodes = new ArrayList<IndexFrame>();
                            for (int k = 0; k < levelNodes.size(); k++) {
                                levelNodes.get(k).sort();
                                if (levelNodes.get(k).getType() == 1) {
                                    cnue = false;
                                    for (Chunk ie : levelNodes.get(k).getFrameChunks(s)) {
                                        if (levelNodes.get(k).getType() == 1) {
                                            q.put(ie);
                                        }
                                    }
                                } else {
                                    for (int i = 0; i < levelNodes.get(k).getFrameChunks(s).size(); i++) {
                                        inNodes.add(Instance.getInstance().getFrameById(levelNodes.get(k).getFrameChunks(s).get(i).getHeader().getFramePtr()).getIndexFrame());
                                    }
                                    if (k == levelNodes.size() - 1) {
                                        int lcf = levelNodes.get(k).getLcF();
                                        long lcb = levelNodes.get(k).getLcB();
                                        if (lcf > 0) {
                                            inNodes.add(Instance.getInstance().getFrameById(lcf + lcb).getIndexFrame());
                                        }
                                    }
                                }
                            }
                            levelNodes = inNodes;
                            if (stopped.get()) {
                                cnue = false;
                            }
                        }
                    }
                }
                q.put(tc);
                return true;
            }

            @Override
            public void stop() {
                stopped.set(true);
            }
        };
        return new RetrieveQueue(q, r);
    }

    public Object poll(Session s) {
        try {
            final RetrieveQueue rq = s.getContentQueue(this);
            return rq.poll();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Chunk cpoll(Session s) {
        try {
            final RetrieveQueue rq = s.getContentQueue(this);
            return rq.cpoll();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    protected ArrayList<FrameData> getStream (Map<Long, Long> retrieved, Session s) throws Exception {
        final ArrayList<FrameData> r = new ArrayList<>();
        if (this.isIndex()) { //index table
            throw new InternalException();
        } else {
            final LinkedBlockingQueue<FrameData> bds = Instance.getInstance().getTableById(this.getObjectId()).getFrames();
            boolean cnue = true;
            while (cnue) {
                final FrameData b = bds.take();
                if (b.getObjectId() == 0 && b.getFrameId() == 0) {
                    cnue = false;
                } else {
                    if (retrieved.get(b.getFrameId()) == null) {
                        r.add(b);
                    }
                }
            }
        }
        return r;
    }

    public DataChunk getChunkByEntity (Object o, Session s) throws Exception {
        final Class c = o.getClass();
        final ResultSetEntity ca = (ResultSetEntity)c.getAnnotation(ResultSetEntity.class);
        if (ca!=null) { //ResultSet entities ALWAYS insert only, then, no neccessary for find datachunk
            return null;
        }
        if (this.isNoTran()) {
            final java.lang.reflect.Field idf = this.getIdField();
            final Method idMethod = this.getIdmethod();
            if (idf == null) {
                logger.error("No @Id annotated column found for " + this.getName());
            }
            final MapField mf = this.getMapFieldByColumn(idf.getName());
            final IndexField ix = this.getIndexFieldByColumn(idf.getName());
            if (mf != null) {
                return (DataChunk) mf.getMap().get(idmethod.invoke(o, null));
            } else if (ix != null) {
                return (DataChunk) ix.getIndex().getObjectByKey(new IndexElementKey(new Object[]{idMethod.invoke(o, null)}));
            } else {
                final byte[] id = new DataChunkId(o, s).getIdBytes();
                if (id!=null) {
                    final LinkedBlockingQueue<FrameData> bds = Instance.getInstance().getTableById(this.getObjectId()).getFrames();
                    boolean cnue = true;
                    while (cnue) {
                        final FrameData b = bds.take();
                        if (b.getObjectId() == 0 && b.getFrameId() == 0) {
                            cnue = false;
                        } else {
                            for (Chunk dc : b.getDataFrame().getFrameChunks(s)) {
                                if (Arrays.equals(id, ((DataChunk) dc).getSerializedId(s))) {
                                    return (DataChunk) dc;
                                }
                            }
                        }
                    }
                }
            }
        } else {
            final EntityContainer to = (EntityContainer)o;
            final Table idt = getFirstIndexByIdColumn();
            if (to.getDataChunk() == null) {
                //todo need further optimize
                final DataChunkId dcid = new DataChunkId(o, s);
                if (idt != null) {
                    final DataChunk idc = idt.getObjectByKey(new ValueSet(dcid.getId()), s);
                    if (idc == null) {
                        return null;
                    }
                    final IndexChunk ibx = (IndexChunk) idc.getEntity();
                    return ibx.getDataChunk();
                } else {
                    final byte[] id = dcid.getIdBytes();
                    if (id != null) {
                        final LinkedBlockingQueue<FrameData> bds = Instance.getInstance().getTableById(this.getObjectId()).getFrames();
                        boolean cnue = true;
                        while (cnue) {
                            final FrameData b = bds.take();
                            if (b.getObjectId() == 0 && b.getFrameId() == 0) {
                                cnue = false;
                            } else {
                                for (Chunk dc : b.getDataFrame().getFrameChunks(s)) {
                                    if (Arrays.equals(id, ((DataChunk) dc).getSerializedId(s))) {
                                        return (DataChunk) dc;
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                return to.getDataChunk();
            }
        }

        return null;
    }

    public DataChunk getChunkById (long id, Session s) throws Exception {
        final String type = getIdField().getType().getName();
        final Table idt = getFirstIndexByIdColumn();
        final Integer id_ = type.equals("int") || type.equals("java.lang.Integer") ? (int)id : null;
        final ValueSet vs = id_ == null ? (new ValueSet(id)) : (new ValueSet(id_));
        if (idt != null) {
            final DataChunk idc = idt.getObjectByKey(vs, s);
            if (idc==null) {
                return null;
            }
            final IndexChunk ibx = (IndexChunk)idc.getEntity();
            return ibx.getDataChunk();
        } else {
            byte[] iid = null;
            if (type.equals("int")) {
                iid = getBytesFromInt((int) id);
            }
            if (type.equals("long")) {
                iid = getBytesFromLong(id);
            }
            if (type.equals("java.lang.Integer")) {
                iid = append(new byte[]{1}, getBytesFromInt((int) id));
            }
            if (type.equals("java.lang.Long")) {
                iid = append(new byte[]{1}, getBytesFromLong(id));
            }

            if (iid != null) {
                LinkedBlockingQueue<FrameData> bds = Instance.getInstance().getTableById(this.getObjectId()).getFrames();
                boolean cnue = true;
                while (cnue) {
                    final FrameData b = bds.take();
                    if (b.getObjectId() == 0 && b.getFrameId() == 0) {
                        cnue = false;
                    } else {
                        for (Chunk c : b.getDataFrame().getFrameChunks(s)) {
                            if (Arrays.equals(iid, ((DataChunk) c).getSerializedId(s))) {
                                return (DataChunk) c;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    private byte[] getBytesFromInt(int p) {
        final ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(p);
        return bb.array();
    }

    private byte[] getBytesFromLong(long p) {
        final ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(p);
        return bb.array();
    }

    private byte[] append(byte[] b, byte[] toAdd){
        final byte[] res = new byte[b.length + toAdd.length];
        System.arraycopy(b, 0, res, 0, b.length);
        System.arraycopy(toAdd, 0, res, b.length, toAdd.length);
        return res;
    }

    /****************** persistent indexes *******************/

    //rowid used in DataChunk constructor for build standalone indexes
    private synchronized void add (RowId rowid, Object o, Session s, LLT extllt) throws Exception {

        final DataChunk dc = new DataChunk(o, s, rowid);
        final int len = dc.getBytesAmount();
        dc.getHeader().setTran(s.getTransaction());

        boolean cnue = true;
        FrameData target = Instance.getInstance().getFrameById(this.getFileStart()+this.getFrameStart());

        final LLT llt = extllt==null?LLT.getLLT():extllt;

        try {
            while (cnue) {
                if (target.getIndexFrame().getType() == IndexFrame.INDEX_FRAME_LEAF) {
                    cnue = false;
                } else { //node
                    final DataChunk cc = target.getIndexFrame().getChildElementPtr(dc.getDcs());
                    final int parentF = target.getIndexFrame().getFile();
                    final long parentB = target.getIndexFrame().getPointer();
                    if (cc != null) {
                        target = Instance.getInstance().getFrameById(cc.getHeader().getFramePtr());
                        target.setMv(cc.getDcs()); //set non-persitent maxvalue
                    } else {
                        final long lcId = target.getIndexFrame().getLcF() + target.getIndexFrame().getLcB();
                        target = Instance.getInstance().getFrameById(lcId); //get by last child
                        if (target == null) {
                            logger.info("null target returned for frame id " + lcId);
                        }
                    }
                    target.getIndexFrame().setParentF(parentF);
                    target.getIndexFrame().setParentB(parentB);
                }
            }

            cnue = true;

            //store element to target leaf
            FrameData prevtg = null;
            DataChunk dc__ = null;
            while (cnue) {
                final DataChunk dc_ = dc__ == null ? dc : dc__;
                IndexFrame newlist = target.getIndexFrame().add(dc_, this, s, llt);
                if (isNoTran()) {
                    usedSpace(target, target.getUsed() + len, true, s, llt);
                } else {
                    s.getTransaction().storeFrame(target, len, s, llt);
                }
                if (newlist == null) {
                    cnue = false;
                } else {
                    prevtg = target;
                    if (newlist.getDivided() == 1) {
                        dc__ = new DataChunk(newlist.getMaxValue(), s, new RowId(newlist.getFile(), newlist.getPointer(), 0), this);
                    } else {
                        dc__ = new DataChunk(prevtg.getIndexFrame().getMaxValue(), s, new RowId(prevtg.getIndexFrame().getFile(), prevtg.getIndexFrame().getPointer(), 0), this);
                    }
                    if (prevtg.getIndexFrame().getParentF() == 0) { //add parent ElementList - always type 2 (node)
                        final int nfileId = getIndexFileId(prevtg);
//                    target = createNewFrame(prevtg, prevtg.getFile(), IndexFrame.INDEX_FRAME_NODE, s);
                        target = createNewFrame(prevtg, nfileId, IndexFrame.INDEX_FRAME_NODE, 0, false, false, false, s, llt);
                        this.setFileStart(target.getIndexFrame().getFile());
                        this.setFrameStart(target.getIndexFrame().getPointer());
                        s.persist(this, llt);
                    } else {
                        target = Instance.getInstance().getFrameById(prevtg.getIndexFrame().getParentF() + prevtg.getIndexFrame().getParentB()); //get by last child
                    }
                    if (newlist.getDivided() == 0) {
                        target.getIndexFrame().setLcF(newlist.getFile()); //lc must be > 0 (0 is first leaf ElementList)
                        target.getIndexFrame().setLcB(newlist.getPointer()); //lc must be > 0 (0 is first leaf ElementList)
                    }
                }
            }
        } finally {
            if (extllt == null) { llt.commit(); }
        }

        if (!isNoTran()) {
//todo            ((EntityContainer)o).setTransId(dc.getHeader().getTran());
//todo            ((EntityContainer)o).setRowId(dc.getHeader().getRowID());
//todo            ((EntityContainer)o).setDataChunk(dc);
            ((EntityContainer)o).getDataChunk().setIc(dc);
        }
    }

    //todo???
    public int getIndexFileId(FrameData bd) {
        final DataFile[] dfs = bd.getDataFile().isIndex() ? Storage.getStorage().getIndexFiles() : Storage.getStorage().getTempFiles();
        int first = 0;
        int last = 0;
        for (DataFile f : dfs) {
            if (first == 0) {
                first = f.getFileId();
            }
            if (ixFrameCurr.compareAndSet(last, f.getFileId())) {
                return ixFrameCurr.get();
            }
            last = f.getFileId();
        }
        ixFrameCurr.compareAndSet(last, first);
        return ixFrameCurr.get();
    }

    public synchronized void storeFrames(List<SyncFrame> frames, int sourceNodeId, LLT llt, Session s) throws Exception {
        for (SyncFrame b : frames) {
            b.getBd().setStarted(0);
            b.getRFrame().setFrameData(b.getBd());
            b.getBd().setFrame(b.getRFrame());
            if (b.isStarted()) {
                ixstartfs.put(sourceNodeId, b.getBd().getFrameId());
                b.getBd().setStarted(sourceNodeId);
            }
            b.getDf().writeFrame(b.getBd(), b.getBd().getPtr(), b.getBd().getFrame().getFrame(), llt, s);
        }
        for (SyncFrame b : frames) {
            b.getBd().setFrame(null);
        }
    }

    @Deprecated
    public synchronized List<Chunk> getContent(Session s) throws Exception {
        ArrayList<Chunk> res = new ArrayList<Chunk>();
        res.addAll(getLocalContent(this.fileStart+this.frameStart, s));
        for (Map.Entry<Integer, Long> entry : ixstartfs.entrySet()) {
            res.addAll(getLocalContent(entry.getValue(), s));
        }
        return res;
    }

    @Deprecated
    private synchronized List<Chunk> getLocalContent(long start, Session s) throws Exception {
        ArrayList<Chunk> res = new ArrayList<Chunk>();
        ArrayList<IndexFrame> levelNodes = new ArrayList<IndexFrame>();
        boolean cnue = true;
        final FrameData bd = Instance.getInstance().getFrameById(start);
        if (bd == null) {
            return res;
        }
        //frame must be local or remote chain started (RCS)
        if (bd.getFrameId() != bd.getAllocId() && bd.getStarted() == 0) {
            return res;
        }
        IndexFrame el = bd.getIndexFrame();
        el.sort();
        levelNodes.add(el);
        while (cnue) {
            ArrayList<IndexFrame> inNodes = new ArrayList<IndexFrame>();
            for (int k=0; k<levelNodes.size(); k++) {
                levelNodes.get(k).sort();
                if (levelNodes.get(k).getType()==1) {
                    cnue = false;
                    for (Chunk ie : levelNodes.get(k).getFrameChunks(s)) {
                        if (levelNodes.get(k).getType()==1) {
                            res.add(ie);
                        }
                    }
                } else {
                    for (int i=0; i<levelNodes.get(k).getFrameChunks(s).size(); i++) {
                        inNodes.add(Instance.getInstance().getFrameById(levelNodes.get(k).getFrameChunks(s).get(i).getHeader().getFramePtr()).getIndexFrame());
                    }
                    if (k==levelNodes.size()-1) {
                        int lcf = levelNodes.get(k).getLcF();
                        long lcb = levelNodes.get(k).getLcB();
                        if (lcf>0) {
                            inNodes.add(Instance.getInstance().getFrameById(lcf+lcb).getIndexFrame());
                        }
                    }
                }
            }
            levelNodes = inNodes;
        }
        return res;
    }

    private synchronized LinkedBlockingQueue<FrameData> getLeafFrames (Session s) throws InternalException, EmptyFrameHeaderFound {
        final LinkedBlockingQueue<FrameData> q = new LinkedBlockingQueue<>(1000);
        final ArrayList<Long> startfs = new ArrayList<>();
        startfs.add(this.fileStart+this.frameStart);
        for (Map.Entry<Integer, Long> entry : ixstartfs.entrySet()) {
            startfs.add(entry.getValue());
        }
        Runnable r = new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName("interference-retrieve-index-frames-thread");
                try {
                    for (Long start : startfs) {
                        getLocalLeafFrames(q, start, s);
                    }
                    //terminate
                    q.put(new FrameData());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        framepool.submit(r);
        return q;
    }

    private synchronized LinkedBlockingQueue<FrameData> getLocalLeafFrames (LinkedBlockingQueue<FrameData> q, long start, Session s) throws Exception {
        ArrayList<IndexFrame> levelNodes = new ArrayList<IndexFrame>();
        boolean cnue = true;
        FrameData bd = Instance.getInstance().getFrameById(start);
        //frame must be local or remote chain started (RCS)
        if (bd.getFrameId() != bd.getAllocId() && bd.getStarted() == 0) {
            return q;
        }
        IndexFrame el = bd.getIndexFrame();
        el.sort();
        levelNodes.add(el);
        while (cnue) {
            ArrayList<IndexFrame> inNodes = new ArrayList<IndexFrame>();
            for (int k=0; k<levelNodes.size(); k++) {
                //levelNodes.get(k).sort();
                if (levelNodes.get(k).getType()==1) {
                    cnue = false;
                    q.put(Instance.getInstance().getFrameById(levelNodes.get(k).getPtr()));
                } else {
                    for (int i=0; i<levelNodes.get(k).getFrameChunks(s).size(); i++) {
                        inNodes.add(Instance.getInstance().getFrameById(levelNodes.get(k).getFrameChunks(s).get(i).getHeader().getFramePtr()).getIndexFrame());
                    }
                    if (k==levelNodes.size()-1) {
                        int lcf = levelNodes.get(k).getLcF();
                        long lcb = levelNodes.get(k).getLcB();
                        if (lcf>0) {
                            inNodes.add(Instance.getInstance().getFrameById(lcf+lcb).getIndexFrame());
                        }
                    }
                }
            }
            levelNodes = inNodes;
        }
        return q;
    }

    public synchronized String getInfo(Session s) throws Exception {
        ArrayList<IndexFrame> levelNodes = new ArrayList<IndexFrame>();
        boolean cnue = true;
        IndexFrame el = Instance.getInstance().getFrameById(this.fileStart+this.frameStart).getIndexFrame();
        el.sort();
        levelNodes.add(el);
        int nodecnt = 0;
        int leafcnt = 0;
        int nodeamt = 0;
        int leafamt = 0;
        while (cnue) {
            ArrayList<IndexFrame> inNodes = new ArrayList<IndexFrame>();
            for (int k=0; k<levelNodes.size(); k++) {
                levelNodes.get(k).sort();
                if (levelNodes.get(k).getType()==1) {
                    cnue = false;
                    leafcnt++;
                    for (Chunk ie : levelNodes.get(k).getFrameChunks(s)) {
                        if (levelNodes.get(k).getType()==1) {
                            leafamt++;
                        }
                    }
                } else {
                    nodecnt++;
                    for (int i=0; i<levelNodes.get(k).getFrameChunks(s).size(); i++) {
                        nodeamt++;
                        inNodes.add(Instance.getInstance().getFrameById(levelNodes.get(k).getFrameChunks(s).get(i).getHeader().getFramePtr()).getIndexFrame());
                    }
                    if (k==levelNodes.size()-1) {
                        int lcf = levelNodes.get(k).getLcF();
                        long lcb = levelNodes.get(k).getLcB();
                        if (lcf>0) {
                            inNodes.add(Instance.getInstance().getFrameById(lcf+lcb).getIndexFrame());
                        }
                    }
                }
            }
            levelNodes = inNodes;
        }
        return "nframes: "+nodecnt+" nds: "+nodeamt+" lframes: "+leafcnt+" lfs: "+leafamt;
    }


    public synchronized DataChunk getObjectByKey (ValueSet key, Session s) throws Exception {
        final long start = this.fileStart+this.frameStart;
        final DataChunk dc = getLocalObjectByKey(start, key, s);
        if (dc != null) {
            return dc;
        } else {
            for (Map.Entry<Integer, Long> entry : ixstartfs.entrySet()) {
                final DataChunk dc_ = getLocalObjectByKey(entry.getValue(), key, s);
                if (dc_ != null) {
                    return dc_;
                }
            }
        }
        return null;
    }

    public synchronized List<DataChunk> getObjectsByKey (ValueSet key, Session s) throws Exception {
        final long start = this.fileStart+this.frameStart;
        final List<DataChunk> r = getLocalObjectsByKey(start, key, s);
        for (Map.Entry<Integer, Long> entry : ixstartfs.entrySet()) {
            r.addAll(getLocalObjectsByKey(entry.getValue(), key, s));
        }
        return r;
    }

    //for unique indexes
    private synchronized DataChunk getLocalObjectByKey (long start, ValueSet key, Session s) throws Exception {
        boolean cnue = true;
        IndexFrame target = Instance.getInstance().getFrameById(start).getIndexFrame();
        while (cnue) {
            if (target.getType()==1) { //leaf
                cnue = false;
            } else {
                DataChunk cc = target.getChildElementPtr(key);
                if (cc!=null) {
                    target = Instance.getInstance().getFrameById(cc.getHeader().getFramePtr()).getIndexFrame();
                } else {
                    final long lcId = target.getLcF()+target.getLcB();
                    target = Instance.getInstance().getFrameById(lcId).getIndexFrame(); //get by last child
                    if (target == null) {
                        logger.info("null target returned for frame id "+lcId);
                    }
                }
            }
        }
        return target.getObjectByKey(key, s);
    }

    //for non-unique indexes
    private synchronized List<DataChunk> getLocalObjectsByKey (long start, ValueSet key, Session s) throws Exception {
        final List<DataChunk> r = new ArrayList<>();
        boolean cnue = true;
        List<IndexFrame> targets = new ArrayList<>();
        targets.add(Instance.getInstance().getFrameById(start).getIndexFrame());
        while (cnue) {
            List<IndexFrame> ntargets = new ArrayList<>();
            for (IndexFrame target : targets) {
                if (target.getType()==1) { //leaf
                    r.addAll(target.getObjectsByKey(key, s));
                    cnue = false;
                } else {
                    ArrayList<Long> cptr = target.getChildElementsPtr(key);
                    if (cptr.size()>0) {
                        for (Long i : cptr) {
                            ntargets.add(Instance.getInstance().getFrameById(i).getIndexFrame());
                        }
                    }
                    if (target.getLcF()>0) {
                        ntargets.add(Instance.getInstance().getFrameById(target.getLcF()+target.getLcB()).getIndexFrame()); //get by last child
                    }
                }
            }
            targets = ntargets;
        }
        return r;
    }

    //find object(s) by key and remove unique object (param)
    @Deprecated
    private synchronized void removeObjects(ValueSet key, Object o, Session s, LLT llt) throws Exception {
        boolean cnue = true;
        ArrayList<FrameData> targets = new ArrayList<FrameData>();
        targets.add(Instance.getInstance().getFrameById(this.fileStart+this.frameStart));
        while (cnue) {
            ArrayList<FrameData> ntargets = new ArrayList<FrameData>();
            for (FrameData target : targets) {
                if (target.getIndexFrame().getType()==1) { //leaf
                    int len = target.getIndexFrame().removeObjects(key, o);
                    if (isNoTran()) { usedSpace(target, target.getUsed()-len, true, s, llt); } else { s.getTransaction().storeFrame(target, 0-len, s, llt); }
                    cnue = false;
                } else {
                    ArrayList<Long> cptr = target.getIndexFrame().getChildElementsPtr(key);
                    if (cptr.size()>0) {
                        for (Long i : cptr) {
                            ntargets.add(Instance.getInstance().getFrameById(i));
                        }
                    }
                    if (target.getIndexFrame().getLcF()>0) {
                        ntargets.add(Instance.getInstance().getFrameById(target.getIndexFrame().getLcF()+target.getIndexFrame().getLcB())); //get by last child
                    }
                }
            }
            targets = ntargets;
        }
    }

    public int getObjectId() {
        return objectId;
    }

    public int getTableId() {
        return objectId;
    }

    public void setObjectId(int objectId) {
        this.objectId = objectId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getFileStart() {
        return fileStart;
    }

    public void setFileStart(int fileStart) {
        this.fileStart = fileStart;
    }

    public long getFrameStart() {
        return frameStart;
    }

    public void setFrameStart(long frameStart) {
        this.frameStart = frameStart;
    }

    public int getFrameSize() {
        return frameSize;
    }

    public void setFrameSize(int frameSize) {
        this.frameSize = frameSize;
    }

    public int getFileLast() {
        return fileLast;
    }

    public void setFileLast(int fileLast) {
        this.fileLast = fileLast;
    }

    public long getFrameLast() {
        return frameLast;
    }

    public void setFrameLast(long frameLast) {
        this.frameLast = frameLast;
    }

    public long getFrameAmount() {
        return frameAmount.get();
    }

    public void setFrameAmount(long frameAmount) {
        this.frameAmount.set(frameAmount);
    }

    public long getLtran() {
        return ltran;
    }

    public void setLtran(long ltran) {
        this.ltran = ltran;
    }

    public int getParentId() {
        return parentId;
    }

    public void setParentId(int parentId) {
        this.parentId = parentId;
    }

    public int getIdIncrement() {
        return idIncrement;
    }

    public void setIdIncrement(int idIncrement) {
        this.idIncrement = idIncrement;
    }
}
