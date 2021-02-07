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

package su.interference.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.*;
import su.interference.exception.InternalException;

import javax.persistence.*;
import javax.tools.*;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SimplePOJOProxyFactory {
    private static final URLClassLoader ucl = Instance.getUCL();
    private static final ConcurrentHashMap<String, Class> hmap = new ConcurrentHashMap<String, Class>();
    private static final SimplePOJOProxyFactory instance = new SimplePOJOProxyFactory();
    private static final String PROXY_PREFIX = "";
    private final static Logger logger = LoggerFactory.getLogger(SimplePOJOProxyFactory.class);

    private SimplePOJOProxyFactory() { }

    public static SimplePOJOProxyFactory getInstance() { return instance; }

    public synchronized Class register (String name) throws ClassNotFoundException, InternalException, IOException {
        final ClassContainer cc = build(name);
        return compile(cc);
    }

    public synchronized Class compile (ClassContainer source) throws ClassNotFoundException, InternalException, IOException {
        if (source == null) {
            return null;
        }
        logger.debug(source.getSource());

        final JavaCompiler jc = ToolProvider.getSystemJavaCompiler();
        final StandardJavaFileManager fm = jc.getStandardFileManager(null,null,null);
        fm.setLocation(StandardLocation.CLASS_OUTPUT, Arrays.asList(new File(Config.getConfig().DB_PATH)));
        final SimpleJavaFileObject fo = new JavaSourceFromString(source.getPrefix() + PROXY_PREFIX + source.getSimpleName(), source.getSource());
        Iterable<? extends JavaFileObject> units = Arrays.asList(fo);
        final DiagnosticCollector<JavaFileObject> dc = new DiagnosticCollector<JavaFileObject>();
        final JavaCompiler.CompilationTask ct = jc.getTask(null,fm,dc,null,null,units);
        ct.call();

        for (Diagnostic d : dc.getDiagnostics()) {
            logger.debug(d.getCode());
            logger.debug(d.getKind().toString());
            logger.debug(String.valueOf(d.getPosition()));
            logger.debug(String.valueOf(d.getStartPosition()));
            logger.debug(String.valueOf(d.getEndPosition()));
            logger.debug(d.getSource().toString());
            logger.debug(d.getMessage(null));
        }

        final Class<?> pc = ucl.loadClass(source.getPrefix() + PROXY_PREFIX + source.getSimpleName());
        hmap.put(source.getName(), pc);
        return pc;
    }

    public synchronized ClassContainer build (String name) throws ClassNotFoundException, InternalException {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Class c = null;

        try {
            c = cl.loadClass(name);
        } catch (ClassNotFoundException e) {
            c = null;
        }

        if (c == null) {
            c = ucl.loadClass(name);
        }

        if (c == null) {
            throw new RuntimeException("cannot load class "+name);
        }

        final String prefix = c.getPackage().getName()+".";
        final String sname = c.getSimpleName();
        final Entity ca = (Entity)c.getAnnotation(Entity.class);
        final Table ta = (Table)c.getAnnotation(Table.class);
        final SystemEntity sa = (SystemEntity)c.getAnnotation(SystemEntity.class);
        final IndexEntity isa = (IndexEntity)c.getAnnotation(IndexEntity.class);
        final ResultSetEntity rsa = (ResultSetEntity)c.getAnnotation(ResultSetEntity.class);

        if (ca == null) {
            throw new InternalException();
        }
        if (sa != null || isa != null || rsa != null) {
            return null;
        }

        final Field[] f  = c.getDeclaredFields();
        final Method[] m = c.getMethods();

        final StringBuffer sb = new StringBuffer();

        sb.append("package ");
        sb.append(c.getPackage().getName());
        sb.append(";\n");
        sb.append("\n");
        sb.append("import javax.persistence.*;\n");
        sb.append("import su.interference.core.*;\n");
        sb.append("import su.interference.persistent.Transaction;\n");
        sb.append("\n");
        sb.append("@Entity\n");
        sb.append(getTableAnnotation(ta));
        sb.append("public class ");
        sb.append(PROXY_PREFIX);
        sb.append(sname);
        sb.append(" implements su.interference.core.EntityContainer {\n");
        sb.append("\n");
        sb.append("    @Transient\n");
        sb.append("    public Transaction tran;\n");
        sb.append("    @Transient\n");
        sb.append("    public boolean received;\n");
        sb.append("    @Transient\n");
        sb.append("    public su.interference.core.RowId rowid;\n");
        sb.append("    @Transient\n");
        sb.append("    public su.interference.core.DataChunk dc;\n");
        sb.append("    public boolean getReceived() { return received; }\n");
        sb.append("    public void setReceived(boolean received) { this.received = received; }\n");
        sb.append("    public Transaction getTran() { return tran; }\n");
        sb.append("    public void setTran(Transaction t) { tran = t; }\n");
        sb.append("    public su.interference.core.RowId getRowId() { return rowid; }\n");
        sb.append("    public void setRowId(su.interference.core.RowId r) { rowid = r; }\n");
        sb.append("    public su.interference.core.DataChunk getDataChunk() { return dc; }\n");
        sb.append("    public void setDataChunk(su.interference.core.DataChunk c) { dc = c; }\n");
        sb.append("\n");
        //simple constructor
        sb.append("    public ");
        sb.append(PROXY_PREFIX);
        sb.append(sname);
        sb.append(" () {\n");
        sb.append("    }\n");
        sb.append("\n");
        for (int i=0; i<f.length; i++) {
            Id a1 = f[i].getAnnotation(Id.class);
            Column a2 = f[i].getAnnotation(Column.class);
            DistributedId a3 = f[i].getAnnotation(DistributedId.class);
            GeneratedValue a4 = f[i].getAnnotation(GeneratedValue.class);
            NoCheck a5 = f[i].getAnnotation(NoCheck.class);
            Transient a6 = f[i].getAnnotation(Transient.class);

            if (a1 != null) { sb.append("@Id\n"); }
            if (a2 != null) { sb.append("@Column\n"); }
            if (a3 != null) { sb.append("@DistributedId\n"); }
            if (a4 != null) { sb.append("@GeneratedValue\n"); }
            if (a5 != null) { sb.append("@NoCheck\n"); }
            if (a6 != null) { sb.append("@Transient\n"); }
            sb.append("private ");
            sb.append(f[i].getType().getName());
            sb.append(" ");
            sb.append(f[i].getName());
            sb.append(";\n");

            if (a1!=null||a6==null) {
                String getm = "get"+f[i].getName().substring(0,1).toUpperCase()+f[i].getName().substring(1,f[i].getName().length());
                Class<?>[] pt = null;
                Class<?>[] et = null;
                for (int j=0; j<m.length; j++) {
                    if (getm.equals(m[j].getName())) {
                        pt = m[j].getParameterTypes();
                        et = m[j].getExceptionTypes();
                    }
                }

                //get method
                sb.append("public ");
                sb.append(f[i].getType().getName());
                sb.append(" get");
                sb.append(f[i].getName().substring(0,1).toUpperCase());
                sb.append(f[i].getName().substring(1,f[i].getName().length()));
                sb.append("(");
                String sparam = null;
                if (pt!=null) {
                    for (int k=0; k<pt.length; k++) {
                        if (k>0) {sb.append(", "); }
                        sb.append(pt[k].getName());
                        sb.append(" p");
                        sb.append(k);
                        if (pt[k].getName().equals("su.interference.persistent.Session")) {
                            sparam = "p"+k;
                        }
                    }
                }
                sb.append(")");
                if (et!=null) {
                    for (int k=0; k<et.length; k++) {
                        if (k==0) {sb.append(" throws "); }
                        if (k>0) {sb.append(", "); }
                        sb.append(et[k].getName());
                    }
                }
                sb.append(" {\n");
                if (sparam==null) {
                    sb.append("    su.interference.persistent.Session session = su.interference.persistent.Session.getContextSession();\n");
                } else {
                    sb.append("    su.interference.persistent.Session session = null;\n");
                    sb.append("    if (");
                    sb.append(sparam);
                    sb.append("==null) { session = su.interference.persistent.Session.getContextSession(); } else { session = ");
                    sb.append(sparam);
                    sb.append("; }\n");
                }
                sb.append("    if (session.isStream()||tran==null||(tran!=null&&tran.getTransType()>=su.interference.persistent.Transaction.TRAN_THR)||(tran!=null&&tran.getTransId() == ");
                sb.append("session.getTransaction().getTransId())) {\n");
                sb.append("        return this.");
                sb.append(f[i].getName());
                sb.append(";\n");
                sb.append("    } else {\n");
                sb.append("        ");
                sb.append(PROXY_PREFIX);
                sb.append(sname);
                sb.append(" u = (");
                sb.append(PROXY_PREFIX);
                sb.append(sname);
                sb.append(")dc.getUndoChunk().getDataChunk().getUndoEntity();\n ");
                sb.append("        return u.get");
                sb.append(f[i].getName().substring(0,1).toUpperCase());
                sb.append(f[i].getName().substring(1,f[i].getName().length()));
                sb.append("(");
                if (pt!=null) {
                    for (int k=0; k<pt.length; k++) {
                        if (k>0) {sb.append(", "); }
                        sb.append("p");
                        sb.append(k);
                    }
                }
                sb.append(");\n");
                sb.append("    }\n");
                sb.append("}\n");

                //set method
                if (a2!=null) {
                    String setm = "set"+f[i].getName().substring(0,1).toUpperCase()+f[i].getName().substring(1,f[i].getName().length());
                    pt = null;
                    et = null;
                    for (int j=0; j<m.length; j++) {
                        if (setm.equals(m[j].getName())) {
                            pt = m[j].getParameterTypes();
                            et = m[j].getExceptionTypes();
                        }
                    }
                    sb.append("public void set");
                    sb.append(f[i].getName().substring(0,1).toUpperCase());
                    sb.append(f[i].getName().substring(1,f[i].getName().length()));
                    sb.append("(");
                    sparam = null;
                    if (pt!=null) {
                        for (int k=0; k<pt.length; k++) {
                            if (k>0) {sb.append(", "); }
                            sb.append(pt[k].getName());
                            sb.append(" p");
                            sb.append(k);
                            if (pt[k].getName().equals("su.interference.persistent.Session")) {
                                sparam = "p"+k;
                            }
                        }
                    }
                    sb.append(")");
                    if (et!=null) {
                        for (int k=0; k<et.length; k++) {
                            if (k==0) {sb.append(" throws "); }
                            if (k>0) {sb.append(", "); }
                            sb.append(et[k].getName());
                        }
                    }

                    sb.append(" {\n");
                    if (sparam==null) {
                        sb.append("    su.interference.persistent.Session session = su.interference.persistent.Session.getContextSession();\n");
                    } else {
                        sb.append("    su.interference.persistent.Session session = null;\n");
                        sb.append("    if (");
                        sb.append(sparam);
                        sb.append("==null) { session = su.interference.persistent.Session.getContextSession(); } else { session = ");
                        sb.append(sparam);
                        sb.append("; }\n");
                    }
                    sb.append("    try {\n");
                    sb.append("        this.");
                    sb.append(f[i].getName());
                    sb.append(" = p0;\n");
                    sb.append("    } catch (su.interference.exception.CannotAccessToLockedRecord e) {\n");
                    sb.append("        throw e;\n");
                    sb.append("    } catch (java.lang.Exception e) {\n");
                    sb.append("        e.printStackTrace();\n }");
                    sb.append("    }\n");
                }
            }
        }
        sb.append("}\n");

        return new ClassContainer(sb.toString(), name, prefix, sname);
    }

    private static class JavaSourceFromString extends SimpleJavaFileObject {
        final String code;
        JavaSourceFromString (String name, String code) {
            super(URI.create("string:///" + name.replaceAll("\\.", "/") + Kind.SOURCE.extension), Kind.SOURCE);
            this.code = code;
        }
        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    private String getTableAnnotation(Table table) {
        final StringBuffer sb = new StringBuffer();
        sb.append("@Table(name=\"");
        sb.append(table.name());
        sb.append("\"");
        if (table.indexes() != null && table.indexes().length > 0) {
            sb.append(", indexes={");
            for (Index ix : table.indexes()) {
                sb.append(getIndexAnnotation(ix));
            }
            sb.append("}");
        }
        sb.append(")\n");
        return sb.toString();
    }

    private String getIndexAnnotation(Index index) {
        final StringBuffer sb = new StringBuffer();
        sb.append("@Index(name=\"");
        sb.append(index.name());
        sb.append("\", columnList=\"");
        sb.append(index.columnList());
        sb.append("\", unique=");
        sb.append(index.unique());
        sb.append(")");
        return sb.toString();
    }
}
