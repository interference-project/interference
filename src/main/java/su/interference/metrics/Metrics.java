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

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class Metrics {

    public static final int COUNTER = 1;
    public static final int HISTOGRAM = 2;
    public static final int TIMER = 3;
    public static final int METER = 10;
    private static final ConcurrentHashMap<String, Meter> metrics = new ConcurrentHashMap<String, Meter>();
    private static final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    public static void register(int type, String name) throws Exception {
        if (type == COUNTER) { metrics.put(name, new Counter(name)); }
        if (type == HISTOGRAM) { metrics.put(name, new Histogram(name)); }
        if (type == TIMER) { metrics.put(name, new Timer(name)); }
        if (type == METER) { metrics.put(name, new Meter(name)); }

        ObjectName obj = new ObjectName("su.interference:type="+name+metrics.get(name).getClass().getSimpleName());
        mbs.registerMBean(metrics.get(name), obj);
    }

    public static Meter get(String name) {
        return metrics.get(name);
    }

}
