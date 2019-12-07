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

package su.interference.standalone;

import su.interference.core.Instance;
import su.interference.persistent.Node;
import su.interference.persistent.Session;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class Start {

    public static void main(String args[]) throws Exception {
        Instance instance = Instance.getInstance();
        final Session s = Session.getSession();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                instance.shutdownInstance();
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("stopped");
        }));

        s.setUserId(Session.ROOT_USER_ID);

        instance.startupInstance(s);
    }

}
