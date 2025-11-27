/**
 The MIT License (MIT)

 Copyright (c) 2010-2025 interference

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

package su.interference.mgmt;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class MgmtContainer {
    MgmtColumn mgmtColumn;
    MgmtAction mgmtAction;
    Field field;
    Method method;
    Class c;
    Field idField;

    public MgmtContainer(MgmtColumn mgmtColumn, MgmtAction mgmtAction, Field field, Method method, Class c, Field idField) {
        this.mgmtColumn = mgmtColumn;
        this.mgmtAction = mgmtAction;
        this.field = field;
        this.method = method;
        this.c = c;
        this.idField = idField;
    }

    public MgmtColumn getMgmtColumn() {
        return mgmtColumn;
    }

    public MgmtAction getMgmtAction() {
        return mgmtAction;
    }

    public Field getField() {
        return field;
    }

    public Method getMethod() {
        return method;
    }

    public int getSize() {
        if (mgmtColumn != null) {
            return mgmtColumn.width();
        } else {
            return 10;
        }
    }

    public String getHeader() {
        if (mgmtColumn != null) {
            return mgmtColumn.name();
        } else {
            return "";
        }
    }

    public String getValue(Object o) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        if (this.mgmtColumn == null || this.field == null) {
            return null;
        } else {
            String getName  = "get" + this.field.getName().substring(0,1).toUpperCase() + this.field.getName().substring(1);
            Method m = this.c.getMethod(getName, null);
            Object result = m.invoke(o, null);
            return String.valueOf(result);
        }
    }

    public String getId(Object o) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        if (this.idField == null) {
            return null;
        } else {
            String getName  = "get" + this.idField.getName().substring(0,1).toUpperCase() + this.idField.getName().substring(1);
            Method m = this.c.getMethod(getName, null);
            Object result = m.invoke(o, null);
            return String.valueOf(result);
        }
    }

    public boolean isCommand() {
        return this.method != null && this.mgmtAction != null;
    }
}
