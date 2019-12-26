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

import su.interference.core.Config;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class FrameApiJoin implements Serializable, Callable<FrameApiJoin> {
    private int nodeId;
    private final transient SQLCursor cur;
    private final transient FrameApi bd1;
    private final transient FrameApi bd2;
    private final transient FrameJoinTask frameJoinTask;
    private final long leftAllocId;
    private final long rightAllocId;
    private List<Object> result;

    public FrameApiJoin(int nodeId, SQLCursor cur, FrameApi bd1, FrameApi bd2, Map<Integer, Map<String, FrameApiJoin>> joins) {
        this.nodeId = nodeId;
        this.cur = cur;
        this.bd1 = bd1;
        this.bd2 = bd2;
        this.leftAllocId = bd1.getAllocId();
        this.rightAllocId = bd2 == null ? 0 : bd2.getAllocId();
        if (nodeId == Config.getConfig().LOCAL_NODE_ID) {
            frameJoinTask = cur.buildFrameJoinTask(nodeId, bd1, bd2);
        } else {
            frameJoinTask = null;
            joins.get(nodeId).put(this.getKey(), this);
        }
    }

    public FrameApiJoin call() throws Exception {
        if (nodeId == Config.getConfig().LOCAL_NODE_ID) {
            final FrameJoinTask frameJoinTask = cur.buildFrameJoinTask(nodeId, bd1, bd2);
            result = frameJoinTask.call();
        }
        return this;
    }

    protected String getKey() {
        return this.leftAllocId + "-" + this.rightAllocId;
    }

    @Override
    public boolean equals(Object obj) {
        return this.getKey().equals(((FrameApiJoin)obj).getKey());
    }

    @Override
    public int hashCode() {
        return this.getKey().hashCode();
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public FrameJoinTask getFrameJoinTask() {
        return frameJoinTask;
    }

    public long getLeftAllocId() {
        return leftAllocId;
    }

    public long getRightAllocId() {
        return rightAllocId;
    }

    public List<Object> getResult() {
        return result;
    }

    public void setResult(List<Object> result) {
        this.result = result;
    }
}
