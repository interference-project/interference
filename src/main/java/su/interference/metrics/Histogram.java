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

package su.interference.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class Histogram extends Meter implements HistogramMBean {

    private final AtomicLong cnt = new AtomicLong(0);
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong sum = new AtomicLong(0);
    private final AtomicLong avg = new AtomicLong(0);

    public Histogram(String name) {
        super(name);
    }

    @Override
    public void put(final long value) {
        cnt.incrementAndGet();
        sum.addAndGet(value);
        avg.set(sum.get()/cnt.get());
        if (min.get()>value) min.set(value);
        if (max.get()<value) max.set(value);
    }

    @Override
    public long getCnt() {
        return cnt.get();
    }

    @Override
    public long getMin() {
        return min.get();
    }

    @Override
    public long getMax() {
        return max.get();
    }

    @Override
    public long getSum() {
        return sum.get();
    }

    @Override
    public long getAvg() {
        return avg.get();
    }

    @Override
    public void reset() {
        cnt.set(0);
        min.set(Long.MAX_VALUE);
        max.set(Long.MIN_VALUE);
        sum.set(0);
        avg.set(0);
    }
}
