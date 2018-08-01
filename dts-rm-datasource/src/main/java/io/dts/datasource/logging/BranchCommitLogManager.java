package io.dts.datasource.logging;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import io.dts.datasource.DataSourceHolder;
import io.dts.datasource.struct.ContextStep2;
import io.dts.datasource.struct.UndoLogMode;

public class BranchCommitLogManager extends DtsLogManager {

    private static Logger logger = LoggerFactory.getLogger(BranchCommitLogManager.class);

    @Override
    public void branchCommit(ContextStep2 context) throws SQLException {
        DataSource datasource = DataSourceHolder.getDataSource(context.getDbname());
        DataSourceTransactionManager tm = new DataSourceTransactionManager(datasource);
        TransactionTemplate transactionTemplate = new TransactionTemplate(tm);
        final JdbcTemplate template = new JdbcTemplate(datasource);
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                String deleteSql = String.format("delete from %s where id in (%s) and status = %d", txcLogTableName,
                    context.getGlobalXid(), UndoLogMode.COMMON_LOG.getValue());
                logger.info("delete undo log sql:" + deleteSql);
                template.execute(deleteSql);
            }
        });
    }
}
