package io.dts.datasource.sql.internal.helper;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.regex.Pattern;

import io.dts.datasource.sql.DtsDataSource;
import io.dts.datasource.sql.internal.StatementAdaper;
import io.dts.datasource.parser.struct.SqlType;

public final class StatementHelper {

    private static final Pattern SELECT_FOR_UPDATE_PATTERN =
        Pattern.compile("^select\\s+.*\\s+for\\s+update.*$", Pattern.CASE_INSENSITIVE);
    private final StatementAdaper statement;

    private final DtsDataSource dataSource;

    private final String sql;

    public StatementHelper(DtsDataSource dataSource, StatementAdaper abstractDtsStatement, String sql) {
        this.statement = abstractDtsStatement;
        this.dataSource = dataSource;
        this.sql = sql;
    }

    public SqlType getSqlType() throws SQLException {
        SqlType sqlType = null;
        String noCommentsSql = sql;
        if (sql.contains("/*")) {
            noCommentsSql = StringUtils.stripComments(sql, "'\"", "'\"", true, false, true, true).trim();
        }
        if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, "select")) {
            if (noCommentsSql.toLowerCase().contains(" for ")
                && SELECT_FOR_UPDATE_PATTERN.matcher(noCommentsSql).matches()) {
                throw new SQLException(
                    "only select, insert, update, delete,replace,truncate,create,drop,load,merge sql is supported");
            } else {
                sqlType = SqlType.SELECT;
            }
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, "insert")) {
            sqlType = SqlType.INSERT;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, "update")) {
            sqlType = SqlType.UPDATE;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, "delete")) {
            sqlType = SqlType.DELETE;
        } else {
            throw new SQLException(
                "only select, insert, update, delete,replace,truncate,create,drop,load,merge sql is supported");
        }
        return sqlType;
    }

    public DtsDataSource getDataSource() {
        return dataSource;
    }

    public String getSql() {
        return sql;
    }

    public StatementAdaper getStatement() {
        return statement;
    }

    private static class StringUtils {

        private static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor) {
            return startsWithIgnoreCaseAndWs(searchIn, searchFor, 0);
        }

        private static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor, int beginPos) {
            if (searchIn == null) {
                return searchFor == null;
            }

            int inLength = searchIn.length();

            for (; beginPos < inLength; beginPos++) {
                if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
                    break;
                }
            }

            return startsWithIgnoreCase(searchIn, beginPos, searchFor);
        }

        private static boolean startsWithIgnoreCase(String searchIn, int startAt, String searchFor) {
            return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
        }

        @SuppressWarnings("unused")
        private static String stripComments(String src, String stringOpens, String stringCloses,
            boolean slashStarComments, boolean slashSlashComments, boolean hashComments, boolean dashDashComments) {
            if (src == null) {
                return null;
            }
            StringBuffer buf = new StringBuffer(src.length());
            StringReader sourceReader = new StringReader(src);
            int contextMarker = Character.MIN_VALUE;
            boolean escaped = false;
            int markerTypeFound = -1;
            int ind = 0;
            int currentChar = 0;
            try {
                while ((currentChar = sourceReader.read()) != -1) {

                    if (false && currentChar == '\\') {
                        escaped = !escaped;
                    } else if (markerTypeFound != -1 && currentChar == stringCloses.charAt(markerTypeFound)
                        && !escaped) {
                        contextMarker = Character.MIN_VALUE;
                        markerTypeFound = -1;
                    } else if ((ind = stringOpens.indexOf(currentChar)) != -1 && !escaped
                        && contextMarker == Character.MIN_VALUE) {
                        markerTypeFound = ind;
                        contextMarker = currentChar;
                    }

                    if (contextMarker == Character.MIN_VALUE && currentChar == '/'
                        && (slashSlashComments || slashStarComments)) {
                        currentChar = sourceReader.read();
                        if (currentChar == '*' && slashStarComments) {
                            int prevChar = 0;
                            while ((currentChar = sourceReader.read()) != '/' || prevChar != '*') {
                                if (currentChar == '\r') {

                                    currentChar = sourceReader.read();
                                    if (currentChar == '\n') {
                                        currentChar = sourceReader.read();
                                    }
                                } else {
                                    if (currentChar == '\n') {

                                        currentChar = sourceReader.read();
                                    }
                                }
                                if (currentChar < 0)
                                    break;
                                prevChar = currentChar;
                            }
                            continue;
                        } else if (currentChar == '/' && slashSlashComments) {
                            while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r'
                                && currentChar >= 0);
                        }
                    } else if (contextMarker == Character.MIN_VALUE && currentChar == '#' && hashComments) {
                        // Slurp up everything until the newline
                        while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0);
                    } else if (contextMarker == Character.MIN_VALUE && currentChar == '-' && dashDashComments) {
                        currentChar = sourceReader.read();

                        if (currentChar == -1 || currentChar != '-') {
                            buf.append('-');

                            if (currentChar != -1) {
                                buf.append(currentChar);
                            }

                            continue;
                        }

                        // Slurp up everything until the newline

                        while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0);
                    }

                    if (currentChar != -1) {
                        buf.append((char)currentChar);
                    }
                }
            } catch (IOException ioEx) {
                // we'll never see this from a StringReader
            }
            return buf.toString();
        }
    }
}
