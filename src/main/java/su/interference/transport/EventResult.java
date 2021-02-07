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

package su.interference.transport;

import su.interference.sql.FrameApiJoin;

import java.io.Serializable;
import java.util.List;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class EventResult implements Serializable {
    private final int result;
    private final Object resultObject;
    private final long slaveCursorid;
    private final List<FrameApiJoin> resultSet;
    private final transient TransportChannel channel;
    private final Exception exception;

    public EventResult() {
        this.result = 0;
        this.resultObject = null;
        this.slaveCursorid = 0;
        this.resultSet = null;
        this.exception = null;
        this.channel = null;
    }

    public EventResult(int result, Object resultObject, long slaveCursorid, List<FrameApiJoin> resultSet, Exception exception, TransportChannel channel) {
        this.result = result;
        this.resultObject = resultObject;
        this.slaveCursorid = slaveCursorid;
        this.resultSet = resultSet;
        this.exception = exception;
        this.channel = channel;
    }

    public int getResult() {
        return result;
    }

    public Object getResultObject() {
        return resultObject;
    }

    public long getSlaveCursorid() {
        return slaveCursorid;
    }

    public List<FrameApiJoin> getResultSet() {
        return resultSet;
    }

    public Exception getException() {
        return exception;
    }

    public TransportChannel getChannel() {
        return channel;
    }
}
