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

package su.interference.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class Config {

    private static final String P_LOCAL_NODE_ID = "local.node.id";
    private static final String P_DB_PATH = "db.path";
    private static final String P_JOURNAL_PATH = "journal.path";
    private static final String P_CLUSTER_NODES = "cluster.nodes";
    private static final String P_REGISTER_CLASSES = "auto.register.classes";
    private static final String P_MMPORT = "mmport";
    private static final String P_RMPORT = "rmport";
    private static final String P_FRAMESIZE = "frame.size";
    private static final String P_FRAMESIZE2 = "frame.size.ix";
    private static final String P_FILES_AMOUNT = "files.amount";
    private static final String P_DISKIO_MODE = "diskio.mode";
    private static final String P_SYNC_LOCK_ENABLE = "sync.lock.enable";
    private static final String P_SYNC_PERIOD = "sync.period";
    private static final String P_RETRIEVE_QUEUE_SIZE = "retrieve.queue.size";
    private static final String P_RETRIEVE_THREADS_AMOUNT = "retrieve.threads.amount";
    private static final String P_CODEPAGE = "codepage";
    private static final String P_DATEFORMAT = "dateformat";
    // transport
    private static final String P_REMOTE_SYNC_TIMEOUT = "transport.sync.timeout";
    private static final String P_READ_BUFFER_SIZE = "transport.read.buffer";
    private static final String P_WRITE_BUFFER_SIZE = "transport.write.buffer";
    // cleanup
    private static final String P_TRANS_CLEANUP_TIMEOUT = "cleanup.tx.timeout";
    private static final String P_CLEANUP_ENABLE = "cleanup.enable";
    private static final String P_CLEANUP_TIMEOUT = "cleanup.frames.timeout";
    private static final String P_CLEANUP_PROTECTION_THR = "cleanup.data.threshold";
    private static final String P_IX_CLEANUP_PROTECTION_THR = "cleanup.ix.threshold";
    private static final String P_HEAP_USE_THR_DATA = "cleanup.heap.data.threshold";
    private static final String P_HEAP_USE_THR_INDX = "cleanup.heap.ix.threshold";
    private static final String P_HEAP_USE_THR_TEMP = "cleanup.heap.temp.threshold";
    private static final String P_HEAP_USE_THR_UNDO = "cleanup.heap.undo.threshold";
    private static final int MAX_NODE_ID = 64;
    private static final int MAX_FILES_AMOUNT = 32;

    private static final int LOCAL_NODE_ID_DEFAULT=1;
    private static final String DB_PATH_DEFAULT=System.getProperty("user.home");
    private static final String JOURNAL_PATH_DEFAULT=System.getProperty("user.home");
    private static final String[] CLUSTER_NODES_DEFAULT=new String[]{};
    private static final String[] REGISTER_CLASSES_DEFAULT=new String[]{};
    private static final int MMPORT_DEFAULT=8086;
    private static final int RMPORT_DEFAULT=8050;
    private static final int FRAMESIZE_DEFAULT=16384;
    private static final int FRAMESIZE2_DEFAULT=16384;
    private static final int FILES_AMOUNT_DEFAULT=4;
    private static final String DISKIO_MODE_DEFAULT="rws";
    private static final boolean SYNC_LOCK_ENABLE_DEFAULT=false;
    private static final int SYNC_PERIOD_DEFAULT=2000;
    private static final int RETRIEVE_QUEUE_SIZE_DEFAULT=100000;
    private static final int RETRIEVE_THREADS_AMOUNT_DEFAULT=8;
    private static final String CODEPAGE_DEFAULT="UTF8";
    private static final String DATEFORMAT_DEFAULT="dd.MM.yyyy";
    // transport
    private static final int REMOTE_SYNC_TIMEOUT_DEFAULT = 60000;
    private static final int READ_BUFFER_SIZE_DEFAULT = 33554432;
    private static final int WRITE_BUFFER_SIZE_DEFAULT = 33554432;
    // cleanup
    private static final int TRANS_CLEANUP_TIMEOUT_DEFAULT = 5000;
    private static final boolean CLEANUP_ENABLE_DEFAULT = true;
    private static final int CLEANUP_TIMEOUT_DEFAULT = 3000;
    private static final int CLEANUP_PROTECTION_THR_DEFAULT = 1000;
    private static final int IX_CLEANUP_PROTECTION_THR_DEFAULT = 2000;
    private static final int HEAP_USE_THR_DATA_DEFAULT = 60;
    private static final int HEAP_USE_THR_INDX_DEFAULT = 60;
    private static final int HEAP_USE_THR_TEMP_DEFAULT = 40;
    private static final int HEAP_USE_THR_UNDO_DEFAULT = 50;

    public final int LOCAL_NODE_ID;
    public final String DB_PATH;
    public final String JOURNAL_PATH;
    public final String[] CLUSTER_NODES;
    public final String[] REGISTER_CLASSES;
    public final int MMPORT;
    public final int RMPORT;
    public final int FRAMESIZE;
    public final int FRAMESIZE2;
    public final int FILES_AMOUNT;
    public final String DISKIO_MODE;
    public final boolean SYNC_LOCK_ENABLE;
    public final int SYNC_PERIOD;
    public final int RETRIEVE_QUEUE_SIZE;
    public final int RETRIEVE_THREADS_AMOUNT;
    public final String CODEPAGE;
    public final String DATEFORMAT;
    // transport
    public final int REMOTE_SYNC_TIMEOUT;
    public final int READ_BUFFER_SIZE;
    public final int WRITE_BUFFER_SIZE;
    // cleanup
    public final int TRANS_CLEANUP_TIMEOUT;
    public final boolean CLEANUP_ENABLE;
    public final int CLEANUP_TIMEOUT;
    public final int CLEANUP_PROTECTION_THR;
    public final int IX_CLEANUP_PROTECTION_THR;
    public final int HEAP_USE_THR_DATA;
    public final int HEAP_USE_THR_INDX;
    public final int HEAP_USE_THR_TEMP;
    public final int HEAP_USE_THR_UNDO;
    // internal
    public final int TEST_DISTRIBUTE_MODE = 1;
    public final int CHECK_AVAIL_FRAME_TIMEOUT = 3000;

    private final Properties p;

    public static final String[] cps = new String[]{"Cp858","Cp437","Cp775","Cp850","Cp852","Cp855","Cp857","Cp862","Cp866","ISO8859_1","ISO8859_2","ISO8859_4","ISO8859_5","ISO8859_7",
                         "ISO8859_9","ISO8859_13","ISO8859_15","KOI8_R","KOI8_U","ASCII","UTF8","UTF-16","UnicodeBigUnmarked","UnicodeLittleUnmarked","UTF_32","UTF_32BE","UTF_32LE",
                         "UTF_32BE_BOM","UTF_32LE_BOM","Cp1250","Cp1251","Cp1252","Cp1253","Cp1254","Cp1257","UnicodeBig","Cp737","Cp874","UnicodeLittle"};

    public static final String[] dfs = new String[]{"dd.MM.yyyy", "dd.MM.yyyy HH:mm", "dd.MM.yyyy HH:mm:ss",
                                                    "MM.dd.yyyy", "MM.dd.yyyy HH:mm", "MM.dd.yyyy HH:mm:ss",
                                                    "yyyy.MM.dd", "yyyy.MM.dd HH:mm", "yyyy.MM.dd HH:mm:ss"};

    public static final int[] bss = new int[]{4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288};
    private final static Logger logger = LoggerFactory.getLogger(Config.class);
    private static Config config;

    public static synchronized Config getConfig() {
        if (config==null) {
            config = new Config();
        }
        return config;
    }

    private Config() {
        p = new Properties();
        boolean ok = false;
        try {
            String cname = System.getProperty("su.interference.config");
            if (cname!=null) {
                p.load(new FileInputStream(new File("config/"+cname)));
            } else {
                p.load(new FileInputStream(new File("config/properties")));
            }
            ok = true;
        } catch (IOException e) {
            logger.warn("exception occured when trying to read configuration file");
        }
        if (ok) {
            DB_PATH = validatePath(p.getProperty(P_DB_PATH));
            JOURNAL_PATH = validatePath(p.getProperty(P_JOURNAL_PATH));
            LOCAL_NODE_ID = validateNodeId(p.getProperty(P_LOCAL_NODE_ID));
            CLUSTER_NODES = validateClusterNodes(p.getProperty(P_CLUSTER_NODES));
            REGISTER_CLASSES = validateRegisterClasses(p.getProperty(P_REGISTER_CLASSES));
            MMPORT = validateMMPort(p.getProperty(P_MMPORT));
            RMPORT = validateRMPort(p.getProperty(P_RMPORT));
            FRAMESIZE = validateFrameSize(p.getProperty(P_FRAMESIZE));
            FRAMESIZE2 = validateFrameSize(p.getProperty(P_FRAMESIZE2));
            CODEPAGE = validateCodePage(p.getProperty(P_CODEPAGE));
            DATEFORMAT = validateDateFormat(p.getProperty(P_DATEFORMAT));
            FILES_AMOUNT = validateFilesAmount(p.getProperty(P_FILES_AMOUNT));
            DISKIO_MODE = validateDiskioMode(p.getProperty(P_DISKIO_MODE));
            SYNC_LOCK_ENABLE = validateSyncLock(p.getProperty(P_SYNC_LOCK_ENABLE));
            SYNC_PERIOD = validateSyncPeriod(p.getProperty(P_SYNC_PERIOD));
            RETRIEVE_QUEUE_SIZE = validateQueueSize(p.getProperty(P_RETRIEVE_QUEUE_SIZE));
            RETRIEVE_THREADS_AMOUNT = validateThreadsAmount(p.getProperty(P_RETRIEVE_THREADS_AMOUNT));
            // transport
            REMOTE_SYNC_TIMEOUT = validateInt(p.getProperty(P_REMOTE_SYNC_TIMEOUT), "remote sync timeout", REMOTE_SYNC_TIMEOUT_DEFAULT);
            READ_BUFFER_SIZE = validateInt(p.getProperty(P_READ_BUFFER_SIZE), "read buffer size", READ_BUFFER_SIZE_DEFAULT);;
            WRITE_BUFFER_SIZE = validateInt(p.getProperty(P_WRITE_BUFFER_SIZE), "write buffer size", WRITE_BUFFER_SIZE_DEFAULT);;
            // cleanup
            TRANS_CLEANUP_TIMEOUT = validateInt(p.getProperty(P_TRANS_CLEANUP_TIMEOUT), "tx cleanup timeout", TRANS_CLEANUP_TIMEOUT_DEFAULT);
            CLEANUP_ENABLE = validateEnableFlag(p.getProperty(P_CLEANUP_ENABLE), "cleanup enable", CLEANUP_ENABLE_DEFAULT);
            CLEANUP_TIMEOUT = validateInt(p.getProperty(P_CLEANUP_TIMEOUT), "cleanup timeout", CLEANUP_TIMEOUT_DEFAULT);
            CLEANUP_PROTECTION_THR = validateInt(p.getProperty(P_CLEANUP_PROTECTION_THR), "data cleanup threshold", CLEANUP_PROTECTION_THR_DEFAULT);
            IX_CLEANUP_PROTECTION_THR = validateInt(p.getProperty(P_IX_CLEANUP_PROTECTION_THR), "index cleanup threshold", IX_CLEANUP_PROTECTION_THR_DEFAULT);
            HEAP_USE_THR_DATA = validatePercent(p.getProperty(P_HEAP_USE_THR_DATA), "heap data cleanup threshold", HEAP_USE_THR_DATA_DEFAULT);
            HEAP_USE_THR_INDX = validatePercent(p.getProperty(P_HEAP_USE_THR_INDX), "heap index cleanup threshold", HEAP_USE_THR_INDX_DEFAULT);
            HEAP_USE_THR_TEMP = validatePercent(p.getProperty(P_HEAP_USE_THR_TEMP), "heap temp cleanup threshold", HEAP_USE_THR_TEMP_DEFAULT);
            HEAP_USE_THR_UNDO = validatePercent(p.getProperty(P_HEAP_USE_THR_UNDO), "heap undo cleanup threshold", HEAP_USE_THR_UNDO_DEFAULT);
        } else {
            logger.warn("use default configuration values");
            DB_PATH = DB_PATH_DEFAULT;
            JOURNAL_PATH = JOURNAL_PATH_DEFAULT;
            LOCAL_NODE_ID = LOCAL_NODE_ID_DEFAULT;
            CLUSTER_NODES = CLUSTER_NODES_DEFAULT;
            REGISTER_CLASSES = REGISTER_CLASSES_DEFAULT;
            MMPORT = MMPORT_DEFAULT;
            RMPORT = RMPORT_DEFAULT;
            FRAMESIZE = FRAMESIZE_DEFAULT;
            FRAMESIZE2 = FRAMESIZE2_DEFAULT;
            CODEPAGE = CODEPAGE_DEFAULT;
            DATEFORMAT = DATEFORMAT_DEFAULT;
            FILES_AMOUNT = FILES_AMOUNT_DEFAULT;
            DISKIO_MODE = DISKIO_MODE_DEFAULT;
            SYNC_LOCK_ENABLE = SYNC_LOCK_ENABLE_DEFAULT;
            SYNC_PERIOD = SYNC_PERIOD_DEFAULT;
            RETRIEVE_QUEUE_SIZE = RETRIEVE_QUEUE_SIZE_DEFAULT;
            RETRIEVE_THREADS_AMOUNT = RETRIEVE_THREADS_AMOUNT_DEFAULT;
            // transport
            REMOTE_SYNC_TIMEOUT = REMOTE_SYNC_TIMEOUT_DEFAULT;
            READ_BUFFER_SIZE = READ_BUFFER_SIZE_DEFAULT;
            WRITE_BUFFER_SIZE = WRITE_BUFFER_SIZE_DEFAULT;
            // cleanup
            TRANS_CLEANUP_TIMEOUT = TRANS_CLEANUP_TIMEOUT_DEFAULT;
            CLEANUP_ENABLE = CLEANUP_ENABLE_DEFAULT;
            CLEANUP_TIMEOUT = CLEANUP_TIMEOUT_DEFAULT;
            CLEANUP_PROTECTION_THR = CLEANUP_PROTECTION_THR_DEFAULT;
            IX_CLEANUP_PROTECTION_THR = IX_CLEANUP_PROTECTION_THR_DEFAULT;
            HEAP_USE_THR_DATA = HEAP_USE_THR_DATA_DEFAULT;
            HEAP_USE_THR_INDX = HEAP_USE_THR_INDX_DEFAULT;
            HEAP_USE_THR_TEMP = HEAP_USE_THR_TEMP_DEFAULT;
            HEAP_USE_THR_UNDO = HEAP_USE_THR_UNDO_DEFAULT;
        }
        System.setProperty("com.sun.management.jmxremote.port","8111");
        System.setProperty("com.sun.management.jmxremote.authenticate","false");
        System.setProperty("com.sun.management.jmxremote.ssl","false");
    }

    private static String[] validateClusterNodes(String value) {
        ArrayList<String> l = new ArrayList<>();
        if (value!=null) {
            StringTokenizer st = new StringTokenizer(value, ",");
            while (st.hasMoreTokens()) {
                l.add(st.nextToken());
            }
        }
        return l.toArray(new String[]{});
    }

    private static String[] validateRegisterClasses(String value) {
        ArrayList<String> l = new ArrayList<>();
        if (value!=null) {
            StringTokenizer st = new StringTokenizer(value, ",");
            while (st.hasMoreTokens()) {
                l.add(st.nextToken());
            }
        }
        return l.toArray(new String[]{});
    }

    private static boolean validateSyncLock(String value) {
        if ("true".equals(value)) {
            return true;
        }
        if ("false".equals(value)) {
            return false;
        }
        logger.warn("sync.lock.enable value is not valid - use default");
        return SYNC_LOCK_ENABLE_DEFAULT;
    }

    private static boolean validateEnableFlag(String value, String description, boolean def) {
        if ("true".equals(value)) {
            return true;
        }
        if ("false".equals(value)) {
            return false;
        }
        logger.warn(description + " value is not valid - use default");
        return def;
    }

    private static int validateNodeId(String value) {
        try {
            final int id = Integer.valueOf(value);
            if (id >= 1 && id <= MAX_NODE_ID) {
                return id;
            }
        } catch(NumberFormatException e) {
            logger.warn("node id " + value + " is not valid - use default");
        }
        return LOCAL_NODE_ID_DEFAULT;
    }

    private static String validatePath(String path) {
        if (path==null||path.equals("")) {
            logger.warn("path is not valid - use default");
        }
        return path;
    }

    private static int validateFrameSize(String value) {
        try {
            final int bsize = Integer.valueOf(value);
            for (int bs : bss) {
                if (bs == bsize) {
                    return bsize;
                }
            }
        } catch (NumberFormatException e) {
            logger.warn("frame size " + value + " is not valid - use default");
        }
        return FRAMESIZE_DEFAULT;
    }

    private static int validateMMPort(String value) {
        try {
            final int port = Integer.valueOf(value);
            if (port >= 1 && port <= 65536) {
                return port;
            }
        } catch (NumberFormatException e) {
            logger.warn("management console port " + value + " is not valid - use default");
        }
        return MMPORT_DEFAULT;
    }

    private static int validateRMPort(String value) {
        try {
            final int port = Integer.valueOf(value);
            if (port >= 1 && port <= 65536) {
                return port;
            }
        } catch (NumberFormatException e) {
            logger.warn("remote port " + value + " is not valid - use default");
        }
        return RMPORT_DEFAULT;
    }

    private static String validateCodePage(String cp) {
        for (String c : cps) {
            if (c.equals(cp)) {
                return cp;
            }
        }
        logger.warn("codepage '"+cp+"' is not valid - use default");
        return CODEPAGE_DEFAULT;
    }

    private static String validateDateFormat(String df) {
        for (String d : dfs) {
            if (d.equals(df)) {
                return df;
            }
        }
        logger.warn("dateformat '"+df+"' is not valid - use default");
        return DATEFORMAT_DEFAULT;
    }

    private static int validateFilesAmount(String value) {
        try {
            final int id = Integer.valueOf(value);
            if (id >= 1 && id <= MAX_FILES_AMOUNT) {
                return id;
            }
        } catch(NumberFormatException e) {
            logger.warn("files amount " + value + " is not valid - use default");
        }
        return FILES_AMOUNT_DEFAULT;
    }

    private static int validateThreadsAmount(String value) {
        try {
            final int id = Integer.valueOf(value);
            if (id >= 1 && id <= MAX_FILES_AMOUNT) {
                return id;
            }
        } catch(NumberFormatException e) {
            logger.warn("threads amount value is not valid - use default");
        }
        return RETRIEVE_THREADS_AMOUNT_DEFAULT;
    }

    private static String validateDiskioMode(String mode) {
        if (mode==null||mode.equals("")) {
            logger.error("diskio mode is not valid - use default");
            return DISKIO_MODE_DEFAULT;
        }
        if (mode.toUpperCase().equals("SYNC")) {
            return "rws";
        } else if (mode.toUpperCase().equals("ASYNC")) {
            return "rw";
        } else {
            logger.warn("diskio mode is not valid - use default");
            return DISKIO_MODE_DEFAULT;
        }
    }

    private static int validateSyncPeriod(String value) {
        try {
            int p = Integer.valueOf(value);
            if (p > 9 && p < 60001) {
                return p;
            }
        } catch(NumberFormatException e) {
            logger.warn("sync period value is not valid - use default value");
        }
        return SYNC_PERIOD_DEFAULT;
    }

    private static int validateQueueSize(String value) {
        try {
            int p = Integer.valueOf(value);
            if (p > 0 && p < 100000000) {
                return p;
            }
        } catch(NumberFormatException e) {
            logger.warn("queue size value is not valid - use default value");
        }
        return RETRIEVE_QUEUE_SIZE_DEFAULT;
    }

    private static int validateInt(String value, String description, int def) {
        try {
            int p = Integer.valueOf(value);
            if (p > 0 && p < Integer.MAX_VALUE) {
                return p;
            }
        } catch(NumberFormatException e) {
            logger.warn(description + " value is not valid - use default value");
        }
        return def;
    }

    private static int validatePercent(String value, String description, int def) {
        try {
            int p = Integer.valueOf(value);
            if (p > 0 && p <= 100) {
                return p;
            }
        } catch(NumberFormatException e) {
            logger.warn(description + " value is not valid - use default value");
        }
        return def;
    }

}
