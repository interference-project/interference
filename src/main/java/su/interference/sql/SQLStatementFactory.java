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

package su.interference.sql;

import su.interference.persistent.Cursor;
import su.interference.persistent.Session;

/**
 * @author Yuriy Glotanov
 * @since 1.0
 */

public class SQLStatementFactory {

    public static SQLStatement getInstance(String sql, Session s) {
        if (sql.toUpperCase().trim().startsWith("SELECT")) {
            return new SQLSelect(sql,s);
        }
        if (sql.toUpperCase().trim().startsWith("PROCESS")) {
            return new SQLSelect(sql,s);
        }
/*
        if (sql.toUpperCase().trim().startsWith("INSERT")) {
            return new SQLInsert(sql,s);
        }
        if (sql.toUpperCase().trim().startsWith("UPDATE")) {
            return new SQLUpdate(sql,s);
        }
        if (sql.toUpperCase().trim().startsWith("DELETE")) {
            return new SQLDelete(sql,s);
        }
*/
        if (sql.toUpperCase().trim().startsWith("ALTER SYSTEM")) {
            return new SQLSystem(sql,s);
        }
        if (sql.toUpperCase().trim().startsWith("CONNECT")) {
            return new SQLSystem(sql,s);
        }
        if (sql.toUpperCase().trim().startsWith("ALTER SESSION")) {
            return new SQLSystem(sql,s);
        }
        if (sql.toUpperCase().trim().startsWith("COMMIT")) {
            return new SQLSystem(sql,s);
        }
        if (sql.toUpperCase().trim().startsWith("ROLLBACK")) {
            return new SQLSystem(sql,s);
        }
        if (sql.toUpperCase().trim().startsWith("FREEZE")) {
            return new SQLSystem(sql,s);
        }
        return null;
    }

    public static SQLStatement getInstance(String sql, Cursor c, Session s) {
        if (sql.toUpperCase().trim().startsWith("SELECT")) {
            return new SQLSelect(sql, c, s);
        }
        if (sql.toUpperCase().trim().startsWith("PROCESS")) {
            return new SQLSelect(sql, c, s);
        }
        return null;
    }
}
