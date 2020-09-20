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

package su.interference.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.Config;
import su.interference.core.Instance;
import su.interference.exception.InternalException;
import su.interference.persistent.Table;

import javax.tools.*;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class IOTProxyFactory {
    private static final URLClassLoader ucl = Instance.getUCL();
    private static final ConcurrentHashMap<String, Class> hmap = new ConcurrentHashMap<String, Class>();
    private static final IOTProxyFactory instance = new IOTProxyFactory();
    private final static Logger logger = LoggerFactory.getLogger(IOTProxyFactory.class);

    private IOTProxyFactory() { }

    public static IOTProxyFactory getInstance() { return instance; }

    public synchronized Class register (Field[] cs, String name, String pname) throws ClassNotFoundException, InternalException, IOException {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        final String p = "su.interference.persistent";
        String sname;

        if (name.indexOf(p)==0) {
            sname = name.substring(p.length()+1, name.length());
        } else {
            throw new InternalException();
        }

        final StringBuffer sb = new StringBuffer();

        sb.append("package su.interference.persistent;\n");
        sb.append("\n");
        sb.append("import javax.persistence.Entity;\n");
        sb.append("import javax.persistence.Id;\n");
        sb.append("import javax.persistence.Column;\n");
        sb.append("import javax.persistence.Transient;\n");
        sb.append("import javax.persistence.GeneratedValue;\n");
        sb.append("import su.interference.core.IndexEntity;\n");
        sb.append("import su.interference.core.IndexChunk;\n");
        sb.append("import su.interference.core.EntityContainer;\n");
        sb.append("import su.interference.core.ResultSetEntity;\n");
        sb.append("import su.interference.mgmt.MgmtColumn;\n");
        sb.append("import su.interference.persistent.Transaction;\n");
        sb.append("\n");
        sb.append("@Entity\n");
        sb.append("@IndexEntity\n");
        sb.append("public class ");
        sb.append(sname);
        sb.append(" implements IndexChunk, EntityContainer {\n");
        sb.append("\n");
        sb.append("    @Transient\n");
        sb.append("    public Transaction tran;\n");
        sb.append("    @Transient\n");
        sb.append("    public boolean received;\n");
        sb.append("    @Transient\n");
        sb.append("    public su.interference.core.RowId rowid;\n");
        sb.append("    @Transient\n");
        sb.append("    public su.interference.core.RowId framePtrRowId;\n");
        sb.append("    @Transient\n");
        sb.append("    public su.interference.core.DataChunk dc;\n");
        sb.append("    public su.interference.core.DataChunk getDataChunk() {\n");
        sb.append("        if (dc == null) {\n");
        sb.append("            final long framePtr = framePtrRowId.getFileId() + framePtrRowId.getFramePointer();\n");
        sb.append("            try {\n");
        sb.append("                dc = (su.interference.core.DataChunk) su.interference.core.Instance.getInstance().getChunkByPointer(framePtr, framePtrRowId.getRowPointer());\n");
        sb.append("            } catch (java.lang.Exception e) {\n");
        sb.append("                throw new java.lang.RuntimeException(e);\n");
        sb.append("            }\n");
        sb.append("        }\n");
        sb.append("        return dc;\n");
        sb.append("    }\n");
        sb.append("    public boolean getReceived() { return received; }\n");
        sb.append("    public void setReceived(boolean received) { this.received = received; }\n");
        sb.append("    public void setDataChunk(su.interference.core.DataChunk c) { dc = c; }\n");
        sb.append("    public Transaction getTran() { return tran; }\n");
        sb.append("    public void setTran(Transaction t) { tran = t; }\n");
        sb.append("    public su.interference.core.RowId getRowId() { return rowid; }\n");
        sb.append("    public void setRowId(su.interference.core.RowId r) { rowid = r; }\n");
        sb.append("    public su.interference.core.RowId getFramePtrRowId() { return framePtrRowId; }\n");
        sb.append("    public void setFramePtrRowId(su.interference.core.RowId r) { framePtrRowId = r; }\n");
        sb.append("\n");

        for (int i=0; i<cs.length; i++) {
            Field f = cs[i];
            sb.append("    @Column\n");
            sb.append("    @MgmtColumn(width=50, show=true, form=false, edit=false)\n");
            sb.append("    private ");
            sb.append(f.getType().getName());
            sb.append(" ");
            sb.append(f.getName());
            sb.append(";\n");
        }
        sb.append("\n");
        //create constructor
        sb.append("    public ");
        sb.append(sname);
        sb.append(" (su.interference.core.DataChunk dc, su.interference.persistent.Session s) {\n");
        sb.append("        this.dc = dc;\n");
        sb.append("        this.framePtrRowId = dc.getHeader().getRowID();\n");
        sb.append("        final ");
        sb.append(pname);
        sb.append(" o = (");
        sb.append(pname);
        sb.append(")dc.getEntity();\n");
        for (int i=0; i<cs.length; i++) {
            Field f = cs[i];
            sb.append("        this.");
            sb.append(f.getName());
            sb.append(" = o.get");
            sb.append(f.getName().substring(0,1).toUpperCase());
            sb.append(f.getName().substring(1,f.getName().length()));
            sb.append("(s);\n");
        }
        sb.append("}\n");
        //simple constructor
        sb.append("    public ");
        sb.append(sname);
        sb.append(" () {\n");
        sb.append("\n");
        sb.append("}\n");

        for (int i=0; i<cs.length; i++) {
            Field f = cs[i];
            sb.append("    public ");
            sb.append(f.getType().getName());
            sb.append(" get");
            sb.append(f.getName().substring(0,1).toUpperCase());
            sb.append(f.getName().substring(1,f.getName().length()));
            sb.append("(su.interference.persistent.Session s) {\n");
            sb.append("      return this.");
            sb.append(f.getName());
            sb.append(";\n");
            sb.append("    }\n");
            sb.append("\n");
            sb.append("    public void set");
            sb.append(f.getName().substring(0,1).toUpperCase());
            sb.append(f.getName().substring(1,f.getName().length()));
            sb.append("(");
            sb.append(f.getType().getName());
            sb.append(" ");
            sb.append(f.getName());
            sb.append(", su.interference.persistent.Session s) {\n");
            sb.append("      this.");
            sb.append(f.getName());
            sb.append(" = ");
            sb.append(f.getName());
            sb.append(";\n");
            sb.append("    }\n");
            sb.append("\n");
        }
        sb.append("\n");
        sb.append("}\n");

        final String s = sb.toString();
        logger.debug(s);

        final JavaCompiler jc = ToolProvider.getSystemJavaCompiler();
        final StandardJavaFileManager fm = jc.getStandardFileManager(null,null,null);
        fm.setLocation(StandardLocation.CLASS_OUTPUT, Arrays.asList(new File(Config.getConfig().DB_PATH)));

        final SimpleJavaFileObject fo = new JavaSourceFromString(name, s.toString());
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
        final Class<?> pc = ucl.loadClass("su.interference.persistent."+sname);
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
