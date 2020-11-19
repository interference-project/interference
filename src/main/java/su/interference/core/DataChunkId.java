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

import su.interference.persistent.Session;
import su.interference.exception.InternalException;
import su.interference.serialize.CustomSerializer;

import javax.persistence.Entity;
import javax.persistence.Id;
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
    public DataChunkId (Object o, Session s) throws IOException, InvocationTargetException, NoSuchMethodException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class c = o.getClass();
        final SystemEntity sa = (SystemEntity)c.getAnnotation(SystemEntity.class);
        final TransEntity ta = (TransEntity)c.getAnnotation(TransEntity.class);
        Entity ea = (Entity)c.getAnnotation(Entity.class);
        if (ta!=null) {
            //for Transactional Wrapper Entity we must get superclass (original Entity class)
            c = c.getSuperclass();
            ea = (Entity)c.getAnnotation(Entity.class);
        }
        if (ea==null) {
            throw new InternalException();
        }
        final Field[] f = c.getDeclaredFields();
        for (int i=0; i<f.length; i++) {
            final Id a = f[i].getAnnotation(Id.class);
            if (a!=null) {
                if (sa!=null) {
                    final Method z = c.getMethod("get"+f[i].getName().substring(0,1).toUpperCase()+f[i].getName().substring(1,f[i].getName().length()), null);
                    id = z.invoke(o, null);
                    idb = sr.serialize(f[i].getType().getName(), id);
                } else {
                    final Method z = c.getMethod("get"+f[i].getName().substring(0,1).toUpperCase()+f[i].getName().substring(1,f[i].getName().length()), new Class<?>[]{Session.class});
                    id = z.invoke(o, new Object[]{s});
                    idb = sr.serialize(f[i].getType().getName(), id);
                }
            }
        }
    }

}
