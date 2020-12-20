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

package su.interference.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.*;
import su.interference.exception.InternalException;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Transient;
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

public class POJOProxyFactory {
    private static final URLClassLoader ucl = Instance.getUCL();
    private static final ConcurrentHashMap<String, Class> hmap = new ConcurrentHashMap<String, Class>();
    private static final POJOProxyFactory instance = new POJOProxyFactory();
    private static final String PROXY_PREFIX = "$P";
    private final static Logger logger = LoggerFactory.getLogger(POJOProxyFactory.class);

    private POJOProxyFactory() { }

    public static POJOProxyFactory getInstance() { return instance; }

    public synchronized Class register (String name) throws ClassNotFoundException, InternalException, IOException {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Class c = null;

        try {
            c = cl.loadClass(name);
        } catch (ClassNotFoundException e) {
            c = null;
        }

        if (c==null) {
            c = ucl.loadClass(name);
        }

        final String sname = c.getSimpleName();
        final String prefix = name.substring(0, name.length() - sname.length());
        final Entity ca = (Entity)c.getAnnotation(Entity.class);
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
        final Constructor<?>[] cts = c.getConstructors();

        final StringBuffer sb = new StringBuffer();

        sb.append("package ");
        sb.append(prefix.substring(0, prefix.length()-1));
        sb.append(";\n");
        sb.append("\n");
        sb.append("import javax.persistence.Entity;\n");
        sb.append("import javax.persistence.Transient;\n");
        sb.append("import su.interference.core.TransEntity;\n");
        sb.append("import su.interference.persistent.Transaction;\n");
        sb.append("\n");
        sb.append("@Entity\n");
        sb.append("@TransEntity\n");
        sb.append("public class ");
        sb.append(PROXY_PREFIX);
        sb.append(sname);
        sb.append(" extends ");
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
        sb.append("    public String toString() { return super.toString(); }\n");
        sb.append("\n");
        for (Constructor<?> ct : cts) {
            Class<?>[] cps = ct.getParameterTypes();
            if (cps.length==0) {
                //simple constructor
                sb.append("    public ");
                sb.append(PROXY_PREFIX);
                sb.append(sname);
                sb.append(" () {\n");
                sb.append("    }\n");
                sb.append("\n");
            } else {
                //constructor with params
                sb.append("    public ");
                sb.append(PROXY_PREFIX);
                sb.append(sname);
                sb.append(" (");
                for (int k=0; k<cps.length; k++) {
                    if (k>0) {sb.append(", "); }
                    sb.append(cps[k].getName());
                    sb.append(" p");
                    sb.append(k);
                }
                sb.append(") {\n");
                sb.append("        super(");
                for (int k=0; k<cps.length; k++) {
                    if (k>0) {sb.append(", "); }
                    sb.append("p");
                    sb.append(k);
                }
                sb.append(");\n");
                sb.append("    }\n");
            }
        }

        sb.append("\n");
        for (int i=0; i<f.length; i++) {
            Id a1 = f[i].getAnnotation(Id.class);
            Column a2 = f[i].getAnnotation(Column.class);
            Transient ta = f[i].getAnnotation(Transient.class);

            if (a1!=null||ta==null) {
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
                sb.append("        return super.get");
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
                sb.append("    } else {\n");
                sb.append("        ");
                sb.append(name);
                sb.append(" u = (");
                sb.append(name);
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
                    sb.append("        super.set");
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
                    sb.append("    } catch (su.interference.exception.CannotAccessToLockedRecord e) {\n");
                    sb.append("        throw e;\n");
                    sb.append("    } catch (java.lang.Exception e) {\n");
                    sb.append("        e.printStackTrace();\n }");
                    sb.append("    }\n");
                }
            }
        }
        sb.append("}\n");

        final String s = sb.toString();

        logger.debug(s);

        final JavaCompiler jc = ToolProvider.getSystemJavaCompiler();
        final StandardJavaFileManager fm = jc.getStandardFileManager(null,null,null);
        fm.setLocation(StandardLocation.CLASS_OUTPUT, Arrays.asList(new File(Config.getConfig().DB_PATH)));
        final SimpleJavaFileObject fo = new JavaSourceFromString(prefix + PROXY_PREFIX + sname, s.toString());
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


        final Class<?> pc = ucl.loadClass(prefix + PROXY_PREFIX + sname);
        hmap.put(name, pc);
        return pc;
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

}
