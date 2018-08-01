/*
 * Copyright 2014-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.dts.datasource.logging;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.springframework.jdbc.core.PreparedStatementCallback;

import io.dts.common.context.DtsXID;
import io.dts.common.util.BlobUtil;
import io.dts.datasource.struct.ContextStep2;
import io.dts.parser.struct.TxcRuntimeContext;

public class DtsLogManager {

    protected static final String txcLogTableName = "dts_undo_log";

    private final BranchRollbackLogManager rollbackLogManager;

    private final BranchCommitLogManager commitLogManager;

    public static DtsLogManager getInstance() {
        return DtsLogManagerHolder.instance;
    }

    protected DtsLogManager() {
        this.rollbackLogManager = new BranchRollbackLogManager();
        this.commitLogManager = new BranchCommitLogManager();
    }

    public void branchCommit(ContextStep2 context) throws SQLException {
        commitLogManager.branchCommit(context);
    }

    public void branchRollback(ContextStep2 context) throws SQLException {
        rollbackLogManager.branchRollback(context);
    }

    public Integer insertUndoLog(final Connection connection, final TxcRuntimeContext txcContext) throws SQLException {
        String xid = txcContext.getXid();
        long branchID = txcContext.getBranchId();
        long globalXid = DtsXID.getGlobalXID(xid, branchID);
        String serverAddr = txcContext.getServer();
        StringBuilder insertSql = new StringBuilder("INSERT INTO ");
        insertSql.append(txcLogTableName);
        insertSql.append("(id, xid, branch_id, rollback_info, ");
        insertSql.append("gmt_create, gmt_modified, status, server)");
        insertSql.append(" VALUES(");
        insertSql.append("?,"); // id
        insertSql.append("?,"); // xid
        insertSql.append("?,"); // branch_id
        insertSql.append("?,"); // rollback_info
        insertSql.append("?,"); // gmt_create
        insertSql.append("?,"); // gmt_modified
        insertSql.append(txcContext.getStatus()); // status
        insertSql.append(",?)"); // server
        return LogManagerHelper.executeSql(connection, insertSql.toString(), new PreparedStatementCallback<Integer>() {
            @Override
            public Integer doInPreparedStatement(final PreparedStatement pst) throws SQLException {
                pst.setLong(1, globalXid);
                pst.setString(2, xid);
                pst.setLong(3, branchID);
                pst.setBlob(4, BlobUtil.string2blob(txcContext.encode()));
                java.sql.Timestamp currentTime = new java.sql.Timestamp(System.currentTimeMillis());
                pst.setTimestamp(5, currentTime);
                pst.setTimestamp(6, currentTime);
                pst.setString(7, serverAddr);
                return pst.executeUpdate();
            }
        });

    }

    private static class DtsLogManagerHolder {
        private static DtsLogManager instance = new DtsLogManager();
    }
}
