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

import su.interference.core.DataChunk;
import su.interference.persistent.Session;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class GenericResultImpl implements GenericResult {

    private DataChunk dataChunk;

    public DataChunk getDataChunk(Session s) throws Exception {
        if (dataChunk == null) {
            dataChunk = new DataChunk(this, s);
        }
        return dataChunk;
    }

    public Object getValueByName(String name) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method m = this.getClass().getMethod("get"+name.substring(0,1).toUpperCase()+name.substring(1,name.length()), null);
        return m.invoke(this, null);
    }

    @Override
    public String toString() {
        final Method[] ms = this.getClass().getMethods();
        final StringBuffer  sb = new StringBuffer();
        try {
            for (Method m : ms) {
                if (m.getName().startsWith("get") && m.getParameterTypes().length == 0) {
                    sb.append(m.getName().substring(3).substring(0, 1).toLowerCase());
                    sb.append(m.getName().substring(3).substring(1));
                    sb.append(":");
                    sb.append(m.invoke(this, null));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

}
