package io.dts.datasource.sql.internal.helper;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dts.common.context.DtsContext;
import io.dts.common.exception.DtsException;
import io.dts.datasource.sql.DtsConnection;
import io.dts.parser.DtsVisitorFactory;
import io.dts.parser.struct.RollbackInfor;
import io.dts.parser.struct.TxcTable;
import io.dts.parser.vistor.ITxcVisitor;

public abstract class AbstractExecutor {

    public <T> T executeStatement(final StatementHelper statementUnit, final ExecuteCallback<T> executeCallback)
        throws Exception {
        return execute(statementUnit, Collections.emptyList(), executeCallback);
    }

    public <T> T executePreparedStatement(final StatementHelper preparedStatementUnits, final List<Object> parameters,
        final ExecuteCallback<T> executeCallback)

        throws Exception {
        return execute(preparedStatementUnits, parameters, executeCallback);
    }

    private <T> T execute(StatementHelper baseStatementUnit, final List<Object> parameterSet,
        final ExecuteCallback<T> executeCallback) throws Exception {
        T result = executeInternal(baseStatementUnit, parameterSet, executeCallback);
        return result;
    }

    private <T> T executeInternal(final StatementHelper baseStatementUnit, final List<Object> parameterSet,
        final ExecuteCallback<T> executeCallback) throws Exception {
        try {
            ExecutorRunner commiter = new ExecutorRunner(baseStatementUnit, parameterSet);
            commiter.beforeExecute();
            T result = executeCallback.execute(baseStatementUnit);
            commiter.afterExecute();
            return result;
        } catch (final SQLException ex) {
            throw new DtsException(ex);
        }

    }

    static interface ExecuteCallback<T> {

        T execute(StatementHelper baseStatementUnit) throws Exception;
    }

    private static class ExecutorRunner {
        private static final Logger logger = LoggerFactory.getLogger(ExecutorRunner.class);

        private ITxcVisitor txcVisitor;

        private StatementHelper stateModel;

        public ExecutorRunner(StatementHelper stateModel, final List<Object> parameterSet) throws SQLException {
            this.stateModel = stateModel;
            DtsConnection txcConnection = stateModel.getStatement().getDtsConnection();
            this.txcVisitor = DtsVisitorFactory.createSqlVisitor(txcConnection.getDataSource().getDatabaseType(),
                txcConnection.getRawConnection(), stateModel.getSql(), parameterSet);

        }

        public TxcTable beforeExecute() throws SQLException {
            if (!DtsContext.getInstance().inTxcTransaction()) {
                return null;
            }
            TxcTable nRet = null;
            switch (stateModel.getSqlType()) {
                case DELETE:
                case UPDATE:
                case INSERT:
                    this.txcVisitor.buildTableMeta();
                    txcVisitor.executeAndGetFrontImage(stateModel.getStatement().getRawStatement());
                    break;
                default:
                    break;
            }
            return nRet;
        }

        public TxcTable afterExecute() throws SQLException {
            if (!DtsContext.getInstance().inTxcTransaction()) {
                return null;
            }

            TxcTable nRet = null;
            switch (stateModel.getSqlType()) {
                case DELETE:
                case UPDATE:
                case INSERT:
                    txcVisitor.executeAndGetRearImage(stateModel.getStatement().getRawStatement());
                    insertUndoLog();
                    break;
                default:
                    break;
            }
            return nRet;
        }

        private void insertUndoLog() throws SQLException {
            if (txcVisitor.getTableOriginalValue().getLinesNum() == 0
                && txcVisitor.getTablePresentValue().getLinesNum() == 0) {
                String errorInfo = "null result error:" + txcVisitor.getInputSql();
                logger.error("insertUndoLog", errorInfo);
                throw new DtsException(3333, errorInfo);
            }
            RollbackInfor txcLog = new RollbackInfor();
            txcLog.setSql(txcVisitor.getInputSql());
            txcLog.setSqlType(txcVisitor.getSqlType());
            txcLog.setSelectSql(txcVisitor.getSelectSql());
            txcLog.setOriginalValue(txcVisitor.getTableOriginalValue());
            txcLog.setPresentValue(txcVisitor.getTablePresentValue());
            switch (txcVisitor.getSqlType()) {
                case DELETE:
                    txcLog.setWhereCondition(txcVisitor.getWhereCondition(txcVisitor.getTableOriginalValue()));
                    break;
                case UPDATE:
                    txcLog.setWhereCondition(txcVisitor.getWhereCondition(txcVisitor.getTableOriginalValue()));
                    break;
                case INSERT:
                    txcLog.setWhereCondition(txcVisitor.getWhereCondition(txcVisitor.getTablePresentValue()));
                    break;
                default:
                    throw new DtsException("unknown error");
            }
            txcLog.txcLogChecker();
            stateModel.getStatement().getDtsConnection().getTxcContext().addInfor(txcLog);
        }
    }

}
