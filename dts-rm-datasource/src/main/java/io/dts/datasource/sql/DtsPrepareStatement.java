package io.dts.datasource.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.dts.common.context.DtsContext;
import io.dts.datasource.sql.internal.PrepareStatementAdaper;
import io.dts.datasource.sql.internal.helper.PreparedStatementExecutor;

public class DtsPrepareStatement extends PrepareStatementAdaper {

    public DtsPrepareStatement(final DtsConnection dtsConnection, final PreparedStatement statement,
        String sql) {
        super(dtsConnection, statement);
        setTargetSql(sql);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return getRawStatement().executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        try {
            return new PreparedStatementExecutor(createStatementHelper(getTargetSql()), getParameters()).executeUpdate();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean execute() throws SQLException {
        try {
            return new PreparedStatementExecutor(createStatementHelper(getTargetSql()), getParameters()).execute();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void addBatch() throws SQLException {
        if (DtsContext.getInstance().inTxcTransaction()) {
            throw new UnsupportedOperationException("unsupport add batch in dts transaction");
        }
        getRawStatement().addBatch();
    }

    @Override
    public void addBatch(final String sql) throws SQLException {
        if (DtsContext.getInstance().inTxcTransaction()) {
            throw new UnsupportedOperationException("unsupport add batch in dts transaction");
        }
        getRawStatement().addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        if (DtsContext.getInstance().inTxcTransaction()) {
            throw new UnsupportedOperationException("unsupport clear batch in dts transaction");
        }
        getRawStatement().clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (DtsContext.getInstance().inTxcTransaction()) {
            throw new UnsupportedOperationException("unsupport execute batch in dts transaction");
        }
        return getRawStatement().executeBatch();
    }
}
