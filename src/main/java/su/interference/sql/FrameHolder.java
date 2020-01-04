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

package su.interference.sql;

import su.interference.core.Instance;
import su.interference.persistent.FrameData;
import su.interference.persistent.Table;
import su.interference.exception.InternalException;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class FrameHolder {

    private final AtomicReference<FrameData>[] cframes;
    private final AtomicInteger ccntr = new AtomicInteger(0);
    private final boolean persistent;

    public FrameHolder(ResultSet t) {
        if (t != null && t.getClass().getName().equals("su.interference.persistent.Table")) {
            Table tt = (Table)t;
            cframes = new AtomicReference[tt.getLbs().length];
            for (int i=0; i<tt.getLbs().length; i++) {
                cframes[i] = new AtomicReference<FrameData>();
                cframes[i].set(tt.getLbs()[i].getBd());
            }
            persistent = true;
        } else {
            persistent = false;
            cframes = null;
        }
    }

    public synchronized FrameData getFrame(final boolean done) throws InternalException {
        if (!persistent) return null;
        for (int i=0; i<cframes.length; i++) {
            if (cframes[i].get().getNextFrameId()>0) {
                return cframes[i].getAndSet(Instance.getInstance().getFrameById(cframes[i].get().getNextFrameId()));
            }
        }
        if (done) {
            if (ccntr.get()<cframes.length) {
                if (cframes[ccntr.get()].get().getNextFrameId()==0) {
                    return cframes[ccntr.getAndIncrement()].get();
                } else {
                    throw new InternalException();
                }
            }
        }
        return null;
    }



}
