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

package su.interference.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.core.Config;
import su.interference.core.Instance;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class TransportServer {

    private final static Logger logger = LoggerFactory.getLogger(TransportServer.class);
    private static TransportServer transportServer;
    private final AtomicBoolean started = new AtomicBoolean(true);
    private static final TransportContext transportContext = TransportContext.getInstance();
    private static final int amount = Config.getConfig().CLUSTER_NODES.length;
    private final ExecutorService pool = Executors.newCachedThreadPool();

    private TransportServer() {
        for (int i = 0; i < amount; i++) {
            try {
                final int serverPort = Config.getConfig().RMPORT + i;
                startServer(pool, serverPort, started);
            } catch(IOException e){
                e.printStackTrace();
            }
        }
    }

    private TransportServer(int port) {
        try {
            startServer(pool, port, started);
        } catch(IOException e){
            logger.error("start event server", e);
        }
    }

    private static void startServer(ExecutorService pool, int port, AtomicBoolean started) throws IOException {
        final ServerSocket serverSocket = new ServerSocket(port);
        pool.submit(new Runnable() {
            @Override
            public void run() {
                logger.info("event server started on port " + port);
                Thread.currentThread().setName("interference-event-server-thread-"+port+"-"+Thread.currentThread().getId());
                while (started.get()) {
                    try {
                        serverSocket.setSoTimeout(10000);
                        final Socket socket = serverSocket.accept();
                        logger.info("host address: "+socket.getInetAddress().getHostAddress());
                        final ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(socket.getInputStream(), Config.getConfig().READ_BUFFER_SIZE)) {
                            @Override
                            protected Class<?> resolveClass(ObjectStreamClass objectStreamClass)
                                    throws IOException, ClassNotFoundException {

                                Class<?> clazz = Class.forName(objectStreamClass.getName(), false, Instance.getUCL());
                                if (clazz != null) {
                                    return clazz;
                                } else {
                                    return super.resolveClass(objectStreamClass);
                                }
                            }
                        };
                        pool.submit(new Runnable() {
                            @Override
                            public void run() {
                                boolean running = true;
                                Thread.currentThread().setName("interference-transport-processor-thread-"+port+"-"+Thread.currentThread().getId());
                                logger.info("transport message processor started on port "+port);
                                while (running) {
                                    try {
                                        final InetAddress inetAddress = socket.getInetAddress();
                                        final Object transportMessage = ois.readObject();
                                        transportContext.onMessage((TransportMessage) transportMessage, inetAddress);
                                    } catch (EOFException eof) {
                                        running = false;
                                        logger.warn("event server will be restarted due to " + eof);
                                    } catch (Exception e) {
                                        running = false;
                                        logger.warn("event server will be restarted due to " + e);
                                        logger.error("exception occured during message process ", e);
                                    }
                                }
                            }
                        });
                        logger.debug("transport server iteration");
                    } catch (SocketTimeoutException ste) {
/*
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException ie) {
                                ie.printStackTrace();
                            }
*/
                        logger.debug("server socket timeout exception");
                    } catch (Exception e) {
                        logger.error("start event server", e);
                    }
                }
            }
        });
    }

    public static synchronized TransportServer getInstance() {
        if (transportServer == null) {
            transportServer = new TransportServer();
        }
        return transportServer;
    }

    protected static synchronized TransportServer getInstance(int port) {
        if (transportServer == null) {
            transportServer = new TransportServer(port);
        }
        return transportServer;
    }

    protected void stop() {
        started.set(false);
    }
}
