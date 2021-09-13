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

package su.interference.sql;

import su.interference.core.Chunk;
import su.interference.core.FrameOrderComparator;
import su.interference.persistent.Session;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class ContainerFrame implements FrameApi {
    private final int objectId;
    private List<FrameApi> frames;
    private static final Map<Long, Integer> lastfamt = new ConcurrentHashMap<>();

    public ContainerFrame(int objectId, List<FrameApi> frames) {
        this.objectId = objectId;
        this.frames = frames;
    }

    @Override
    public long getFrameId() {
        return 0;
    }

    @Override
    public long getAllocId() {
        return 0;
    }

    @Override
    public int getObjectId() {
        return objectId;
    }

    @Override
    public int getImpl() {
        return FrameApi.IMPL_CONTAINER;
    }

    @Override
    public ArrayList<Chunk> getFrameChunks(Session s) throws Exception {
        return null;
    }

    @Override
    public ArrayList<Object> getFrameEntities(Session s) throws Exception {
        if (s.isStream()) {
            final ArrayList<Object> res = new ArrayList<>();
            Collections.sort(frames, new FrameOrderComparator());
            final FrameApi first = frames.get(0);
            final FrameApi last = frames.get(frames.size()-1);
            Integer prevamt = lastfamt.get(first.getAllocId());
            int lastamt = 0;
            for (FrameApi f : frames) {
                if (f.getImpl() == FrameApi.IMPL_DATA) {
                    if (f.getAllocId() == first.getAllocId() && prevamt != null) {
                        int i = 0;
                        for (Chunk c : f.getFrameChunks(s)) {
                            if (i > prevamt) {
                                res.add(c.getEntity(s));
                            }
                            i++;
                        }
                    } else {
                        if (f.getAllocId() == last.getAllocId()) {
                            for (Chunk c : f.getFrameChunks(s)) {
                                res.add(c.getEntity(s));
                                lastamt++;
                            }
                        } else {
                            for (Chunk c : f.getFrameChunks(s)) {
                                res.add(c.getEntity(s));
                            }
                        }
                    }
                }
            }
            lastfamt.remove(first.getAllocId());
            lastfamt.put(last.getAllocId(), lastamt);
            return res;
        }
        return null;
    }

    @Override
    public long getFrameOrder() {
        return 0;
    }

    @Override
    public boolean isProcess() {
        return false;
    }

    @Override
    public Class getEventProcessor() {
        return null;
    }
}
