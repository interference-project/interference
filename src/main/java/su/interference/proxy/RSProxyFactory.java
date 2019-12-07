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
import su.interference.sql.SQLColumn;

import javax.tools.*;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class RSProxyFactory {
    private static final URLClassLoader ucl = Instance.getUCL();
    private static final ConcurrentHashMap<String, Class> hmap = new ConcurrentHashMap<String, Class>();
    private static final RSProxyFactory instance = new RSProxyFactory();
    private final static Logger logger = LoggerFactory.getLogger(RSProxyFactory.class);

    private RSProxyFactory() { }

    public static RSProxyFactory getInstance() { return instance; }

    public synchronized Class register (ArrayList<SQLColumn> cs, String name, boolean ixflag) throws ClassNotFoundException, InternalException, IOException {
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
        sb.append("import su.interference.core.SystemEntity;\n");
        sb.append("import su.interference.core.ResultSetEntity;\n");
        sb.append("import su.interference.core.IndexEntity;\n");
        sb.append("import su.interference.core.DisableSync;\n");
        sb.append("import su.interference.mgmt.MgmtColumn;\n");
        sb.append("\n");
        sb.append("@Entity\n");
        sb.append("@SystemEntity\n");
        sb.append("@ResultSetEntity\n");
        for (SQLColumn sqlc : cs) {
            if (sqlc.isMergeIX()) {
                sb.append("@Table(name=\""+sname+"\", indexes={@Index(name=\"MergeIX"+sname+"\", columnList=\""+sqlc.getAlias()+"\", unique=false)})\n");
            }
        }
        if (ixflag) {
            sb.append("@IndexEntity\n");
        }
        sb.append("@DisableSync\n");
        sb.append("public class ");
        sb.append(sname);
        sb.append(" extends su.interference.proxy.GenericResultImpl implements java.io.Serializable {\n");
        sb.append("\n");
        //todo serialVersionUID should be unique for every proxy class
        sb.append("    @Transient\n");
        sb.append("    private final static long serialVersionUID = 6730871208437219890L;\n");
        sb.append("\n");

        for (int i=0; i<cs.size(); i++) {
            Field f = cs.get(i).getColumn();
            sb.append("    @Column\n");
            sb.append("    @MgmtColumn(width=50, show=true, form=false, edit=false)\n");
            sb.append("    private ");
            sb.append(cs.get(i).getResultSetType());
            sb.append(" ");
            sb.append(cs.get(i).getAlias());
            sb.append(";\n");
        }
        sb.append("\n");

        for (int i=0; i<cs.size(); i++) {
            Field f = cs.get(i).getColumn();
            sb.append("    public ");
            sb.append(cs.get(i).getResultSetType());
            sb.append(" get");
            sb.append(cs.get(i).getAlias().substring(0,1).toUpperCase());
            sb.append(cs.get(i).getAlias().substring(1,cs.get(i).getAlias().length()));
            sb.append("() {\n");
            sb.append("      return this.");
            sb.append(cs.get(i).getAlias());
            sb.append(";\n");
            sb.append("    }\n");
            sb.append("\n");
            sb.append("    public void set");
            sb.append(cs.get(i).getAlias().substring(0,1).toUpperCase());
            sb.append(cs.get(i).getAlias().substring(1,cs.get(i).getAlias().length()));
            sb.append("(");
            sb.append(cs.get(i).getResultSetType());
            sb.append(" ");
            sb.append(cs.get(i).getAlias());
            sb.append(") {\n");
            sb.append("      this.");
            sb.append(cs.get(i).getAlias());
            sb.append(" = ");
            sb.append(cs.get(i).getAlias());
            sb.append(";\n");
            sb.append("    }\n");
            sb.append("\n");
        }
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
