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

import su.interference.persistent.Table;
import su.interference.exception.InternalException;

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.lang.reflect.Field;
import java.net.MalformedURLException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class IndexDescript {

    private final Table    t;
    private final String   name;
    private final String[] columns;
    private final Field[]  fields;
    private final boolean  unique;

    public IndexDescript(Table t, String name, String columns, boolean unique) throws InternalException {
        this.t = t;
        Field[] tfs = t.getFields();
        StringTokenizer st = new StringTokenizer(columns,",");
        ArrayList<String> cs = new ArrayList<String>();
        ArrayList<Field> fs = new ArrayList<Field>();
        while (st.hasMoreTokens()) {
            cs.add(st.nextToken().trim());
        }
        for (String cname : cs) {
            boolean chk = false;
            for (Field f : tfs) {
                if (cname.equals(f.getName())) {
                    fs.add(f);
                    chk = true;
                }
            }
            if (!chk) { throw new InternalException(); }
        }
        this.name = name;
        this.columns = cs.toArray(new String[]{});
        this.fields = fs.toArray(new Field[]{});
        this.unique = unique;
    }

    public String getName() {
        return name;
    }

    public String[] getColumns() {
        return columns;
    }

    public Field[] getFields() {
        return fields;
    }

    public Table getT() {
        return t;
    }

    public boolean isUnique() {
        return unique;
    }
}
