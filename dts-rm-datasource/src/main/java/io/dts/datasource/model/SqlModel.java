package io.dts.datasource.model;

import com.alibaba.druid.sql.ast.SQLStatement;

public class SqlModel {

    private String sql;

    private SqlType sqlType;

    private DatabaseType databaseType;

    private SQLStatement sqlStatement;

    public SqlModel(final String sql, final SqlType sqlType, final DatabaseType databaseType,
        final SQLStatement sqlStatement) {
        this.sql = sql;
        this.sqlType = sqlType;
        this.databaseType = databaseType;
        this.sqlStatement = sqlStatement;
    }

    public SQLStatement getSQLStatement() {
        return sqlStatement;
    }

    public SqlType getType() {
        return sqlType;
    }

    public DatabaseType getDatabaseType() {
        return databaseType;
    }

    public String getSql() {
        return sql;
    }

}
