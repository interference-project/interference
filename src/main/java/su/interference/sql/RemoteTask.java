/**
 The MIT License (MIT)

 Copyright (c) 2010-2020 head systems, ltd

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

import su.interference.exception.InternalException;
import su.interference.metrics.Metrics;
import su.interference.persistent.Cursor;
import su.interference.transport.SQLEvent;
import su.interference.transport.TransportContext;
import su.interference.transport.TransportEvent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class RemoteTask implements Callable<Boolean> {
    private final Cursor cur;
    private final int nodeId;
    private final Map<String, FrameApiJoin> joins;
    private final String rightType;
    private final String targetClassName;

    public RemoteTask(Cursor cur, int nodeId, Map<String, FrameApiJoin> joins, String rightType, String targetClassName) {
        this.cur = cur;
        this.nodeId = nodeId;
        this.joins = joins;
        this.rightType = rightType;
        this.targetClassName = targetClassName;
    }

    public Boolean call() throws Exception {
        Thread.currentThread().setName("interference-remote-task-thread-"+Thread.currentThread().getId());
        final TransportEvent transportEvent = new SQLEvent(nodeId, cur.getCursorId(), joins, rightType, 0, null, null, 0, true);
        TransportContext.getInstance().send(transportEvent);
        transportEvent.getLatch().await();
        if (!transportEvent.isFail()) {
            final List<FrameApiJoin> rs = ((SQLEvent) transportEvent).getCallback().getResult().getResultSet();
            if (rs.size() != joins.size()) {
                for (Map.Entry<String, FrameApiJoin> entry : joins.entrySet()) {
                    entry.getValue().setFailed(true);
                }
                throw new InternalException();
            }
            for (FrameApiJoin j : rs) {
                joins.get(j.getKey()).setResultWithCountDown(j.getResult());
                Metrics.get("recordRCount").put(j.getResult().size());
            }
        } else {
            for (Map.Entry<String, FrameApiJoin> entry : joins.entrySet()) {
                entry.getValue().setFailed(true);
            }
        }
        return true;
    }
}
