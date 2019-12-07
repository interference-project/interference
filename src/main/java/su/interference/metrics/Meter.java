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

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class Meter {

    private final String name;

    public Meter (String name) {
        this.name = name;
    }

    public void put() {

    }

    public void put(final long value) {

    }

    public long getCnt() {
        return 0;
    }

    public long getMin() {
        return 0;
    }

    public long getMax() {
        return 0;
    }

    public long getSum() {
        return 0;
    }

    public long getAvg() {
        return 0;
    }

    public void start() {

    }

    public void stop() {

    }

    public String toString() {
        return this.name+": cnt="+getCnt()+", min="+getMin()+", max="+getMax()+", avg="+getAvg();
    }

}
