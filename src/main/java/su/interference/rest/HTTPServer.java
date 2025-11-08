/**
The MIT License (MIT)

Copyright (c) 2010-2025 interference

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

package su.interference.rest;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.Config;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class HTTPServer {
	private final ExecutorService pool = Executors.newFixedThreadPool(1);
	private final static Logger logger = LoggerFactory.getLogger(HTTPServer.class);

    private int port;
    private ServerSocket sock;
    private static final HTTPServer instance = new HTTPServer(Config.getConfig().MMPORT);
    private final AtomicBoolean state = new AtomicBoolean(true);

    public static HTTPServer getInstance() {
    	return instance;
	}

    private HTTPServer(int port) {
    	try {
			this.port = port;
			this.sock = new ServerSocket(port);
			pool.submit(new Runnable() {
				public void run() {
					Thread.currentThread().setName("interference-http-server");
					try {
						while (state.get()) {
							new HTTPSession(sock.accept());
						}
					} catch (IOException e) {
						logger.error("Exception occured during HTTPServer run: ", e);
					}
				}
			});
			logger.info("HTTP Server started on port " + port);
		} catch (IOException e) {
    		logger.error("Exception occured during HTTPServer start", e);
		}
	}

	public void stop() {
		try {
			this.sock.close();
			this.state.set(false);
		} catch (IOException e) {
			logger.error("Exception occured during HTTPServer stop", e);
		}

	}

}

