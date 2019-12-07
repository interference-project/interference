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

package su.interference.sql;

import su.interference.persistent.Session;
import su.interference.core.Instance;
import su.interference.core.DataSet;
import su.interference.sqlexception.SQLException;

import java.util.StringTokenizer;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLSystem implements SQLStatement {

    private String stmt="";
    private String user="";
    private String pass="";

    public SQLSystem (String sql, Session s) {
        if (sql.equals("")) {
            stmt = "defaultAction";
        }
        if (safeTrim(sql,25).equals("ALTER SYSTEM INIT STORAGE")) {
            stmt = "initStorage";
            StringTokenizer st = new StringTokenizer(sql.trim().substring(25,sql.trim().length()),"/");
            if (st.hasMoreTokens()) {
                user = st.nextToken().trim();
            }
            if (st.hasMoreTokens()) {
                pass = st.nextToken().trim();
            }
        }
        if (sql.trim().toUpperCase().equals("ALTER SYSTEM CHECK STORAGE")) {
            stmt = "checkStorage";
        }
        if (sql.trim().toUpperCase().equals("ALTER SYSTEM OPEN STORAGE")) {
            stmt = "openStorage";
        }
        if (sql.trim().toUpperCase().equals("ALTER SYSTEM CLOSE STORAGE")) {
            stmt = "closeStorage";
        }
        if (sql.trim().toUpperCase().equals("ALTER SYSTEM DROP STORAGE")) {
            stmt = "dropStorage";
        }
        if (safeTrim(sql,28).equals("ALTER SYSTEM CREATE INSTANCE")) {
            stmt = "createInstance";
            StringTokenizer st = new StringTokenizer(sql.trim().substring(28,sql.trim().length()),"/");
            if (st.hasMoreTokens()) {
                user = st.nextToken().trim();
            }
            if (st.hasMoreTokens()) {
                pass = st.nextToken().trim();
            }
        }
        if (sql.trim().toUpperCase().equals("ALTER SYSTEM STARTUP INSTANCE")) {
            stmt = "startupInstance";
        }
        if (sql.trim().toUpperCase().equals("ALTER SYSTEM START APP")) {
            stmt = "startApplication";
        }
        if (sql.trim().toUpperCase().equals("ALTER SYSTEM SHUTDOWN INSTANCE")) {
            stmt = "shutdownInstance";
        }
        if (safeTrim(sql,7).equals("CONNECT")) {
            stmt = "connect";
            StringTokenizer st = new StringTokenizer(sql.trim().substring(7,sql.trim().length()),"/");
            if (st.hasMoreTokens()) {
                user = st.nextToken().trim();
            }
            if (st.hasMoreTokens()) {
                pass = st.nextToken().trim();
            }
        }
        if (sql.trim().toUpperCase().equals("ALTER SESSION LOGOUT")) {
            stmt = "logout";
        }
        if (sql.trim().toUpperCase().equals("COMMIT")) {
            stmt = "commit";
        }
        if (sql.trim().toUpperCase().equals("ROLLBACK")) {
            stmt = "rollback";
        }
        if (sql.trim().toUpperCase().equals("FREEZE")) {
            stmt = "freeze";
        }
    }

    public DataSet executeSQL (Session s) throws SQLException {

        String message    = "";
        String action     = "";
        String actionName = "";
        boolean nowrap  = false;
        boolean command = true;
/*
        if (stmt.equals("initStorage")) {
            Storage.getStorage().initStorage();
        }
        if (stmt.equals("checkStorage")) {
            Storage.getStorage().checkStorage();
        }
        if (stmt.equals("openStorage")) {
            Storage.getStorage().openStorage();
        }
        if (stmt.equals("closeStorage")) {
            Storage.getStorage().closeStorage();
        }
        if (stmt.equals("dropStorage")) {
            Storage.getStorage().dropStorage();
        }
*/
        try {
            /*
            if (stmt.equals("createInstance")) {
                Instance.getInstance().createInstance(s);
            }
            */
            if (stmt.equals("startupInstance")) {
                Instance.getInstance().startupInstance(s);
            }
            if (stmt.equals("shutdownInstance")) {
                Instance.getInstance().shutdownInstance(s);
            }
            if (stmt.equals("connect")) {
                if (s.getUserId()==0) {
                    s.setUser(user);
                    s.setPass(pass);
                    s.auth();
                    if (s.getUserId()>0) {
                        if (Instance.getInstance().getSystemState()==Instance.SYSTEM_STATE_UP) {
                            Session check = Instance.getInstance().getSession(s.getSessionId());
                            if (check==null) {
                                s.persist(s); //insert
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (stmt.equals("logout")) {
            try {
                if (s.getUserId()>0) {
                    s.setUserId(0);
                    s.persist(s); //update
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (stmt.equals("commit")) {
            s.commit();
        }
        if (stmt.equals("rollback")) {
            s.rollback();
        }
        if (stmt.equals("freeze")) {
            //Instance;
        }

        return new DataSet(null, message);
    }

    public String safeTrim(String s, int pos) {
        try {
            return s.trim().toUpperCase().substring(0,pos);
        } catch (StringIndexOutOfBoundsException e) {
            return "";
        }
    }

    //compatibility stub implementation by SQLStatement
    public CList getCols() {
        return null;
    }

    public String getSQLErrorText() {
        return null;
    }


}
