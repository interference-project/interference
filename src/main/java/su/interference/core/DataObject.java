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

import su.interference.persistent.Field;
import su.interference.persistent.FrameData;
import su.interference.persistent.Session;
import su.interference.persistent.DataFile;
import su.interference.exception.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public interface DataObject {

    int getObjectId ();
    String getName();
    int getTableId ();
    int getFileStart ();
    long getFrameStart ();
    int getFrameSize ();
    int getFileLast ();
    long getFrameLast ();
    long getFrameAmount();
    void setObjectId (int objectId);
    void setFileStart (int fileStart);
    void setFrameStart (long frameStart);
    void setFrameSize (int frameSize);
    void setFileLast (int fileLast);
    void setFrameLast (long frameLast);
    void setFrameAmount(long frameAmount);
    long getFrameOrder(Session s, LLT llt) throws Exception;
    long getIdValue(Session s, LLT llt) throws Exception;
    long getIncValue(Session s, LLT llt) throws Exception;
    void incFrameAmount ();
    Class getSc();
    Class getGenericClass();
    boolean isNoTran() throws ClassNotFoundException, MalformedURLException;
    boolean isIndex() throws ClassNotFoundException, MalformedURLException;
    Object newInstance() throws IOException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException;
    Object getInstance() throws IOException, InternalException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException;
    void usedSpace (FrameData bd, int used, boolean persist, Session s, LLT llt);
    void addIndexValue (DataChunk dc) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException;
    java.lang.reflect.Field[] getFields() throws ClassNotFoundException, InternalException, MalformedURLException;
    java.lang.reflect.Field getIdField();
    String getIdFieldType();
    String getIdFieldGetter();
    FrameData allocateFrame(DataFile df, DataObject t, Session s, LLT llt) throws Exception;

}
