
package io.dts.datasource.api.statement;

import java.sql.SQLException;

import io.dts.datasource.jdbc.AbstractDtsStatement;
import io.dts.datasource.jdbc.DtsDataSource;
import io.dts.parser.struct.SqlType;

public final class StatementModel {

    private final AbstractDtsStatement statement;

    private final DtsDataSource dataSource;

    private final String sql;

    public StatementModel(DtsDataSource dataSource, AbstractDtsStatement abstractDtsStatement, String sql) {
        super();
        this.statement = abstractDtsStatement;
        this.dataSource = dataSource;
        this.sql = sql;
    }

    public SqlType getSqlType() throws SQLException {
        return SqlTypeParser.getSqlType(sql);
    }

    public DtsDataSource getDataSource() {
        return dataSource;
    }

    public String getSql() {
        return sql;
    }

    public AbstractDtsStatement getStatement() {
        return statement;
    }

}
