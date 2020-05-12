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

import java.util.Date;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class IndexElement implements Comparable {
    final private IndexElementKey key;
    private Object element;
    final private boolean ex;

    public IndexElement (IndexElementKey key, Object element, boolean ex) {
        this.key = key;
        this.element = element;
        this.ex = ex;
    }

    public int compareTo(final Object obj) {
        final IndexElement j = (IndexElement)obj;
        final int res = this.getKey().compareTo(j.getKey());
        if (this.ex && res == 0) {
            if ((Integer)this.getElement() < (Integer)j.getElement()) { return -1; } else if ((Integer)this.getElement() > (Integer)j.getElement()) { return 1; }
            return 0;
        }
        return res;
    }

    public IndexElementKey getKey() {
        return key;
    }

    public Object getElement() {
        return element;
    }

    public void setElement(Object element) {
        this.element = element;
    }

}
