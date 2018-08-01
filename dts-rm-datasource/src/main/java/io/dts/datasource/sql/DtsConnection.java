package io.dts.datasource.sql;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import io.dts.common.context.DtsContext;
import io.dts.common.context.DtsXID;
import io.dts.datasource.log.DtsLogManager;
import io.dts.datasource.sql.internal.ConnectionAdaper;
import io.dts.datasource.sql.model.UndoLogType;
import io.dts.datasource.parser.struct.TxcRuntimeContext;

public class DtsConnection extends ConnectionAdaper {

    private DtsDataSource dtsDataSource;

    private Connection connection;

    private TxcRuntimeContext txcContext;

    public DtsConnection(final DtsDataSource dtsDataSource, final Connection connection) throws SQLException {
        this.dtsDataSource = dtsDataSource;
        this.connection = connection;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new DtsStatement(this, getRawConnection().createStatement());
    }

    @Override
    public PreparedStatement prepareStatement(final String sql) throws SQLException {
        return new DtsPrepareStatement(this, getRawConnection().prepareStatement(sql), sql);
    }

    @Override
    public CallableStatement prepareCall(final String sql) throws SQLException {
        return getRawConnection().prepareCall(sql);
    }

    @Override
    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        getRawConnection().setAutoCommit(autoCommit);
        if (!autoCommit) {
            registerBranch();
        }
    }

    private void registerBranch() throws SQLException {
        if (!DtsContext.getInstance().inTxcTransaction()) {
            return;
        }
        if (getAutoCommit() == true) {
            throw new SQLException("should set autocommit false first.");
        }
        long branchId = dtsDataSource.getResourceManager().register(dtsDataSource.getDbName());
        txcContext = new TxcRuntimeContext();
        txcContext.setBranchId(branchId);
        txcContext.setXid(DtsContext.getInstance().getCurrentXid());
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return getRawConnection().getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        try {
            if (DtsContext.getInstance().inTxcTransaction()) {
                // 日志写库
                txcContext.setServer(DtsXID.getServerAddress(txcContext.getXid()));
                txcContext.setStatus(UndoLogType.COMMON_LOG.getValue());
                DtsLogManager.getInstance().insertUndoLog(this.getRawConnection(), txcContext);
                getRawConnection().commit();
            } else {
                getRawConnection().commit();
            }
        } finally {
            txcContext = null;
        }
    }

    @Override
    public void rollback() throws SQLException {
        try {
            if (DtsContext.getInstance().inTxcTransaction()) {
                getRawConnection().rollback();
            } else {
                getRawConnection().rollback();
            }
        } finally {
            txcContext = null;
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            getRawConnection().close();
        } finally {
            txcContext = null;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return getRawConnection().isClosed();
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
        return new DtsStatement(this, getRawConnection().createStatement(resultSetType, resultSetConcurrency));
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency)
        throws SQLException {
        PreparedStatement preparedStatement =
            getRawConnection().prepareStatement(sql, resultSetType, resultSetConcurrency);
        return new DtsPrepareStatement(this, preparedStatement, sql);
    }

    @Override
    public CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency)
        throws SQLException {
        return getRawConnection().prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency,
        final int resultSetHoldability) throws SQLException {
        return new DtsStatement(this,
            getRawConnection().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency,
        final int resultSetHoldability) throws SQLException {
        PreparedStatement preparedStatement =
            getRawConnection().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        return new DtsPrepareStatement(this, preparedStatement, sql);
    }

    @Override
    public CallableStatement prepareCall(final String sql, final int resultSetType, final int resultSetConcurrency,
        final int resultSetHoldability) throws SQLException {
        return getRawConnection().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException {
        PreparedStatement preparedStatement = getRawConnection().prepareStatement(sql, autoGeneratedKeys);
        return new DtsPrepareStatement(this, preparedStatement, sql);
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException {
        PreparedStatement preparedStatement = getRawConnection().prepareStatement(sql, columnIndexes);
        return new DtsPrepareStatement(this, preparedStatement, sql);
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException {
        PreparedStatement preparedStatement = getRawConnection().prepareStatement(sql, columnNames);
        return new DtsPrepareStatement(this, preparedStatement, sql);
    }

    @Override
    public Connection getRawConnection() throws SQLException {
        return connection;
    }

    @Override
    public DtsDataSource getDataSource() throws SQLException {
        return dtsDataSource;
    }

    @Override
    public TxcRuntimeContext getTxcContext() {
        return txcContext;
    }
}
