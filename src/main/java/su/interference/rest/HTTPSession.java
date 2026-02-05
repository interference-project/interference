/**
The MIT License (MIT)

Copyright (c) 2010-2026 interference

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import su.interference.mgmt.MgmtContainer;
import su.interference.persistent.Session;
import su.interference.core.*;
import su.interference.persistent.Table;
import su.interference.transport.HeartBeatProcess;

import java.lang.reflect.Method;
import java.net.Socket;
import java.io.*;
import java.text.ParseException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static su.interference.core.Types.*;
import static su.interference.core.Types.t_string;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class HTTPSession implements Runnable {

    public static final String HTTP_200_OK = "200 OK";
    public static final String HTTP_301_REDIRECT = "301 Moved Permanently";
    public static final String HTTP_307_REDIRECT = "307 Moved Temporarily";
    public static final String HTTP_302_REDIRECT = "302 Temporary Redirect";
    public static final String HTTP_303_REDIRECT = "303 Found";
    public static final String HTTP_304_NOT_MODIFIED = "304 Not Modified";
    public static final String HTTP_403_FORBIDDEN = "403 Forbidden";
    public static final String HTTP_404_NOT_FOUND = "404 Not Found";
    public static final String HTTP_400_BAD_REQUEST = "400 Bad Request";
    public static final String HTTP_500_INTERNAL_ERROR = "500 Internal Server Error";
    public static final String HTTP_MIME_PLAIN_TEXT = "text/plain";
    public static final String HTTP_MIME_HTML = "text/html";
    public static final String HTTP_MIME_DEFAULT_BINARY = "application/octet-stream";
    public static final String HTTP_MIME_XML = "text/xml";
    public static final String HTTP_MIME_CSV = "text/csv";
    public static final String HTTP_MIME_FORM_URLENCODED = "application/x-www-form-urlencoded";
    public static final String HTTP_MIME_EXCEL = "application/vnd.ms-excel";
    private final static Logger logger = LoggerFactory.getLogger(HTTPSession.class);
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private static int bufsize = 8192;
    private static SimpleDateFormat sdf;
    private static SimpleDateFormat basesdf;

    static {
        sdf = new SimpleDateFormat( "E, d MMM yyyy HH:mm:ss 'GMT'", Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        basesdf = new SimpleDateFormat(Config.getConfig().DATEFORMAT);
    }

    private final Socket sock;

    protected HTTPSession (Socket s) {
        this.sock = s;
        pool.submit(this);
    }

    private HTTPResponse processRequest(Properties params, String uri) throws Exception {
        //process request
        final String sessionId = (String)params.get("session_id");
        final String pageId = (String)params.get("page_id");
        final String tableId = (String)params.get("table_id");
        final String objectId = (String)params.get("object_id");
        final String fileId = (String)params.get("file_id");
        final String type = (String)params.get("type");
        final String action = (String)params.get("action");
        final String command = (String)params.get("command");
        final String param = (String)params.get("param");
        final Session session = sessionId == null ? null : Instance.getInstance().getSession(sessionId);
        final List<String> errors = new ArrayList<>();
        if (session != null && type != null && objectId != null) {
            final Object object = getObjectByTypeAndId(type, objectId);
            if (object != null) {
                Class c = object.getClass();
                Method m = c.getDeclaredMethod(command, String.class, String.class);
                m.invoke(object, param, session.getSessionId());
            }
        }
        if (session != null && tableId != null && objectId != null) {
            final Table t = getTableById(tableId);
            Class c = t.getTableClass();
            if ("apply".equals(action)) {
                final Object object = getObjectByTableIdAndId(tableId, objectId, sessionId);
                final Object o = object == null ? session.newEntity(c) : object;
                List<MgmtContainer> mclist = MCContainer.getMgmtContainers(c, false);
                try {
                    updateEntity(o, mclist, params);
                    session.persist(o);
                    session.commit();
                } catch (Exception e) {
                    errors.add(e.getMessage());
                }
            }
            if ("delete".equals(action)) {
                final Object object = getObjectByTableIdAndId(tableId, objectId, sessionId);
                session.delete(object);
                session.commit();
            }
        }
        try {
            HTTPResponse response = getResponse(session, uri, action, tableId, objectId, pageId, fileId);
            return response;
        } catch (Exception e) {
            logger.error("Exception occured during process HTTP request", e);
        }
        return null;
    }

    private Object getObjectByTypeAndId(String type, String objectId) {
        switch (type) {
            case "Table":
                int id = Integer.valueOf(objectId);
                return Instance.getInstance().getTableById(id);
            case "Session":
                long sid = Integer.valueOf(objectId);
                return Instance.getInstance().getSessionBySid(sid);
            case "Transaction":
                long transId = Integer.valueOf(objectId);
                return Instance.getInstance().getTransactionById(transId);
            case "Cursor":
                long cid = Integer.valueOf(objectId);
                return Instance.getInstance().getCursorById(cid);
            case "TransportChannel":
                int tcid = Integer.valueOf(objectId);
                return HeartBeatProcess.getChannelById(tcid);
            case "Process":
                int pid = Integer.valueOf(objectId);
                return Instance.getInstance().getProcessById(pid);
        }
        return null;
    }

    private Table getTableById(String tableId) {
        if (tableId != null && tableId.matches("\\d+")) {
            int tId = Integer.valueOf(tableId);
            if (tId > 0) {
                return Instance.getInstance().getTableById(tId);
            } else {
                return null;
            }
        } else {
            logger.error("Table identifier incorrect: " + tableId);
            return null;
        }
    }

    private Object getObjectByTableIdAndId(String tableId, String objectId, String sessionId) throws Exception {
        Session s = Instance.getInstance().getSession(sessionId);
        if (s == null) {
            logger.error("Session identifier incorrect: "+sessionId);
            return null;
        }
        int tId = 0;
        long oId = 0;
        if (tableId != null && tableId.matches("\\d+")) {
            tId = Integer.valueOf(tableId);
        } else {
            logger.error("Table identifier incorrect: " + tableId);
            return null;
        }
        if (objectId != null) {
            if (objectId.matches("\\d+")) {
                oId = Long.valueOf(objectId);
            } else {
                logger.error("Object identifier incorrect: " + objectId);
                return null;
            }
        }
        return s.find(tId, oId);
    }

    @Override
    public void run() {
        try	{
            final InputStream is = sock.getInputStream();
            final PipedOutputStream pos = new PipedOutputStream();
            final PipedInputStream pis = new PipedInputStream(pos);
            final BufferedReader in = new BufferedReader(new InputStreamReader(pis));
            final Properties p = new Properties();
            final Properties params = new Properties();
            final Properties header = new Properties();
            final AtomicLong cleft = new AtomicLong(Long.MAX_VALUE);

            if (is == null) return;

            byte[] b = new byte[bufsize];
            final AtomicInteger l = new AtomicInteger(is.read(b, 0, bufsize));
            if (l.get() <= 0) return;

            final BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(b, 0, l.get())));

            parseHeaders(p, params, header, br);

            if (header.getProperty("content-length") != null) {
                try {
                    cleft.set(Long.parseLong(header.getProperty("content-length")));
                } catch (NumberFormatException e) {
                    logger.error("Exception occured during content-length process", e);
                }
            }

            int x = 0;
            boolean check = false;
            while (x < l.get()) {
                if (b[x] == '\r' && b[++x] == '\n' && b[++x] == '\r' && b[++x] == '\n') {
                    check = true;
                    break;
                }
                x++;
            }
            x++;

            if (x < l.get()) {
                pos.write(b, x, l.get() - x);
            }

            if (x < l.get()) {
                long z = cleft.get();
                z -= l.get() - x + 1;
                cleft.set(z);
            } else if (!check || cleft.get() == Long.MAX_VALUE) {
                cleft.set(0);
            }

            Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        byte[] b = new byte[512];
                        while (l.get() >= 0 && cleft.get() > 0) {
                            l.set(is.read(b, 0, 512));
                            cleft.set(cleft.get() - l.get());
                            if (l.get() > 0) pos.write(b, 0, l.get());
                        }
                        pos.close();
                    } catch (IOException e) {
                        logger.error("IOException occured", e);
                    }
                }
            };

            pool.submit(r);

            if (p.getProperty("method").equalsIgnoreCase("POST")) {
                StringTokenizer st = new StringTokenizer(header.getProperty("content-type") , "; ");
                String ctype = st.hasMoreTokens() ? st.nextToken() : "";
                if (ctype.equalsIgnoreCase("multipart/form-data")) {
                    if (!st.hasMoreTokens()) {
                        sendResponse(HTTP_500_INTERNAL_ERROR, "multipart/form-data boundary missing" );
                    }
                    String boundary = st.nextToken();
                    st = new StringTokenizer(boundary, "=" );
                    if (st.countTokens() != 2) {
                        sendResponse(HTTP_500_INTERNAL_ERROR, "multipart/form-data boundary is incorrect" );
                    }

                    st.nextToken();
                    boundary = st.nextToken();

                    boolean cnue = true;
                    boolean fstart = false;
                    String pname = null;
                    while (cnue) {
                        String line = in.readLine();
                        if (line.indexOf(boundary + "--") >= 0) {
                            cnue = false;
                            continue;
                        }
                        if (line.indexOf(boundary) >= 0) {
                            cnue = true;
                            continue;
                        }
                        if (pname != null) {
                            params.put(pname, line);
                            pname = null;
                        }
                        if (fstart) {
                            System.out.println(line);
                        }
                        if (line.indexOf("Content-Disposition: form-data;") >= 0) {
                            StringTokenizer st2 = new StringTokenizer(line, ";");
                            st2.nextToken();
                            String t2 = st2.nextToken();
                            if (t2.indexOf("name=") > 0) {
                                StringTokenizer st3 = new StringTokenizer(t2, "=");
                                st3.nextToken();
                                String paramName = st3.nextToken();
                                pname = paramName.substring(1, paramName.length()-1);
                            } else {
                                sendResponse(HTTP_500_INTERNAL_ERROR, "multipart/form-data is incorrect" );
                            }
                            if (st2.hasMoreTokens()) {
                                t2 = st2.nextToken();
                                if (t2.indexOf("filename=") > 0) {
                                    String ctype2 = in.readLine();
                                    if (ctype2.indexOf("Content-Type:") >= 0) {
                                        StringTokenizer st4 = new StringTokenizer(ctype2, "; ");
                                        st4.nextToken();
                                        ctype2 = st4.nextToken();
                                        if (HTTP_MIME_CSV.equals(ctype2) || HTTP_MIME_EXCEL.equals(ctype2)) {
                                            fstart = true;
                                        } else {
                                            sendResponse(HTTP_500_INTERNAL_ERROR, "Unsupported uploaded file" );
                                        }
                                    } else {
                                        sendResponse(HTTP_500_INTERNAL_ERROR, "multipart/form-data is incorrect" );
                                    }
                                } else {
                                    sendResponse(HTTP_500_INTERNAL_ERROR, "multipart/form-data is incorrect" );
                                }
                                in.readLine();
                            }
                        }
                    }
                } else {
                    StringBuffer sbuf = new StringBuffer();
                    char pbuf[] = new char[512];
                    int read = in.read(pbuf);
                    while (read >= 0 && !(sbuf.lastIndexOf("\r\n") == sbuf.length())) {
                        sbuf.append(pbuf, 0, read);
                        read = in.read(pbuf);
                    }
                    parseParams(sbuf.toString().trim(), params);
                }
            }

            // get response
            HTTPResponse response = processRequest(params, p.getProperty("uri"));

            //send response
            if (response == null) {
                sendResponse(HTTP_500_INTERNAL_ERROR, "empty response returned");
            } else {
                sendResponse (response);
            }

            in.close();
            is.close();
            pis.close();

        } catch (Exception e) {
            logger.error("Exception occured during HTTP session process", e);
            sendResponse(HTTP_500_INTERNAL_ERROR, "500 Internal Error");
        }
    }

    private void parseHeaders (Properties p, Properties params, Properties header, BufferedReader br) {
        try {
            String s = br.readLine();
            if (s == null) return;
            StringTokenizer st = new StringTokenizer(s);
            if (!st.hasMoreTokens()) sendResponse(HTTP_400_BAD_REQUEST, "400 Bad Request");

            String m = st.nextToken();
            p.put("method", m);

            if (!st.hasMoreTokens()) sendResponse(HTTP_400_BAD_REQUEST, "400 Bad Request");

            String uri = st.nextToken();

            int q = uri.indexOf('?');
            if (q >= 0) {
                parseParams(uri.substring(q+1), params);
                uri = parseParam(uri.substring(0, q));
            } else {
                uri = parseParam(uri);
            }

            if (st.hasMoreTokens()) {
                s = br.readLine();
                while (s != null && s.trim().length() > 0) {
                    int i = s.indexOf(':');
                    if (i >= 0) header.put(s.substring(0, i).trim().toLowerCase(), s.substring(i + 1).trim());
                    s = br.readLine();
                }
            }

            p.put("uri", uri);
        } catch (IOException e) {
            logger.error("Exception occured during HTTP session process", e);
            sendResponse(HTTP_500_INTERNAL_ERROR, "500 Internal Error");
        }
    }

    private String parseParam (String s) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '+') {
                sb.append(' ');
            } else if (c == '%') {
                sb.append((char)Integer.parseInt(s.substring(i + 1, i + 3), 16));
                i += 2;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private void parseParams (String params, Properties p) {
        if (params == null) return;

        StringTokenizer st = new StringTokenizer(params, "&");
        while (st.hasMoreTokens()) {
            String e = st.nextToken();
            int d = e.indexOf('=');
            if (d >= 0) p.put(parseParam(e.substring(0, d)).trim(), parseParam(e.substring(d+1)));
        }
    }

    private void sendResponse (String status, String msg) {
        HTTPResponse response = new HTTPResponse(status, HTTP_MIME_PLAIN_TEXT, new ByteArrayInputStream(msg.getBytes()));
        sendResponse (response);
    }

    private void sendResponse (HTTPResponse response) {
        final String status = response.status;
        final String mime = response.mimeType;
        final Properties header = response.getHeader();
        final InputStream data = response.data;

        try {
            if (status == null) {
                logger.error("HTTP response status is empty");
            }

            OutputStream out = sock.getOutputStream();
            PrintWriter pw = new PrintWriter(out);
            pw.print("HTTP/1.1 " + status + " \r\n");

            if (mime != null) pw.print("Content-Type: " + mime + "\r\n");

            if (header == null || header.getProperty("Date") == null) pw.print( "Date: " + sdf.format( new Date()) + "\r\n");

            if (header!=null) {
                Enumeration e = header.keys();
                while (e.hasMoreElements()) {
                    String key = (String)e.nextElement();
                    String value = header.getProperty(key);
                    pw.print(key + ": " + value + "\r\n");
                }
            }

            pw.print("\r\n");
            pw.flush();

            if (data!=null) {
                int amt = data.available();
                byte[] buf = new byte[bufsize];
                while (amt > 0) {
                    int read = data.read(buf, 0, ((amt > bufsize) ? bufsize : amt));
                    if (read <= 0)	break;
                    out.write(buf, 0, read);
                    amt -= read;
                }
            }
            out.flush();
            out.close();
            if (data != null) {
                data.close();
            }
        } catch (IOException e) {
            logger.error("Exception occured during send HTTP response", e);
        }
    }

    private HTTPResponse getResponse (Session session, String uri, String action, String tableId, String objectId, String pageId, String fileId) {
        HTTPResponse res = null;

        if (res == null) {
            uri = uri.trim().replace( File.separatorChar, '/' );
            if ( uri.indexOf( '?' ) >= 0 )
                uri = uri.substring(0, uri.indexOf( '?' ));
        }

        if (res==null) {
            if (!uri.endsWith("/")) {
                uri += "/";
                res = new HTTPResponse( HTTP_301_REDIRECT, HTTP_MIME_HTML, "<html><body>Redirected: <a href=\"" + uri + "\">" + uri + "</a></body></html>");
                res.addHeader("Location", uri);
            }
            //create session
            if (uri.equals("/")) {
                if (session == null) {
                    session = Session.getSession();
                    //todo
                    session.setUserId(Session.ROOT_USER_ID);
                    session.setIpAddress(sock.getInetAddress().getHostAddress());
                    uri += "?session_id="+session.getSessionId();
                    String s = "<html><head>";
                    s+="<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF8\">";
                    s+="<META http-equiv=\"EXPIRES\"      content=\"Wed, 07 Jul 2004 12:34:56 GMT\">";
                    s+="<META http-equiv=\"PRAGMA\"       content=\"NO-CACHE\">";
                    s+="</head><body>Redirected: <a href=\"" + uri + "\">" + uri + "</a></body></html>";
                    res = new HTTPResponse(HTTP_303_REDIRECT, HTTP_MIME_HTML, s);
                    res.addHeader("Location", uri);
                }
            }

            if (res == null)	{
                if (uri.equals("/")) {
                    try {
                        res = new HTTPResponse(HTTP_200_OK, HTTP_MIME_HTML, MCContainer.getMCContent(action, tableId, objectId, pageId, fileId, session));
                    } catch (Exception e) {
                        logger.error("Exception occured during build HTTP response", e);
                        res = new HTTPResponse(HTTP_500_INTERNAL_ERROR, HTTP_MIME_HTML, "Internal Server Error");
                    }
                } else if (uri.equals("/remote/")) {
                    res = new HTTPResponse(HTTP_200_OK, HTTP_MIME_HTML, "OK");
                } else {
                    res = new HTTPResponse(HTTP_400_BAD_REQUEST, HTTP_MIME_PLAIN_TEXT, "BAD");
                }
            }
        }

        res.addHeader("Accept-Ranges", "bytes");
        return res;
    }

    private Object updateEntity(Object o, List<MgmtContainer> mclist, Properties params) throws Exception {
        for (MgmtContainer mgmtContainer : mclist) {
            String value = (String) params.get(mgmtContainer.getField().getName());
            if (value != null && !"".equals(value)) {
                mgmtContainer.getField().setAccessible(true);
                try {
                    switch (mgmtContainer.getField().getType().getName()) {
                        case (p_int):
                        case (t_int):
                        case (c_int):
                            mgmtContainer.getField().setInt(o, Integer.valueOf(value));
                            break;
                        case (p_float):
                        case (t_float):
                            mgmtContainer.getField().setFloat(o, Float.valueOf(value));
                            break;
                        case (p_long):
                        case (t_long):
                        case (c_long):
                            mgmtContainer.getField().setLong(o, Long.valueOf(value));
                            break;
                        case (p_double):
                        case (t_double):
                            mgmtContainer.getField().setDouble(o, Double.valueOf(value));
                            break;
                        case (t_date):
                            mgmtContainer.getField().set(o, basesdf.parse(value));
                            break;
                        case (t_string):
                            mgmtContainer.getField().set(o, value);
                            break;
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Cannot set value " + value + " to field " + mgmtContainer.getField().getName());
                }
            }
        }
        return o;
    }
}
