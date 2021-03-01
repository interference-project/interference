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

package su.interference.core;

import su.interference.persistent.Session;
import su.interference.exception.InternalException;
import su.interference.persistent.Table;
import su.interference.serialize.CustomSerializer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class DataChunkId {

    private volatile byte[] idb;
    private volatile Object id;
    private final CustomSerializer sr = new CustomSerializer();

    public byte[] getIdBytes() {
        return idb;
    }

    public Object getId() {
        return id;
    }

    private byte[] getBytes(Field f, Object o) throws IllegalAccessException, UnsupportedEncodingException, ClassNotFoundException, InternalException, InstantiationException {
        String t = f.getType().getName();
        Object fo = f.get(o);
        return sr.serialize(t, fo);
    }

    //serializer
    @SuppressWarnings("unchecked")
    public DataChunkId (Object o, Table t, Session s) throws IOException, InvocationTargetException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        final Field idf = t.getIdField();
        if (idf != null) {
            final Object o_ = t.isNoTran() ? o : ((EntityContainer) o).getEntity(s);
            final Method z = t.getIdmethod();
            id = z.invoke(o_, null);
            idb = sr.serialize(idf.getType().getName(), id);
        }
    }

}
