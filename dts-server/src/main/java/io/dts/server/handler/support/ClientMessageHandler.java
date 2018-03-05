/*
 * Copyright 2014-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.dts.server.handler.support;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryExpiredListener;

import io.dts.common.api.DtsServerMessageSender;
import io.dts.common.context.DtsXID;
import io.dts.common.exception.DtsException;
import io.dts.common.protocol.header.BeginMessage;
import io.dts.common.protocol.header.BranchCommitMessage;
import io.dts.common.protocol.header.BranchCommitResultMessage;
import io.dts.common.protocol.header.BranchRollBackMessage;
import io.dts.common.protocol.header.BranchRollbackResultMessage;
import io.dts.common.protocol.header.GlobalCommitMessage;
import io.dts.common.protocol.header.GlobalRollbackMessage;
import io.dts.remoting.RemoteConstant;
import io.dts.server.store.DtsLogDao;
import io.dts.server.struct.BranchLog;
import io.dts.server.struct.GlobalLog;
import io.dts.server.struct.GlobalLogState;

/**
 * @author liushiming
 * @version ClientMessageHandler.java, v 0.0.1 2017年9月18日 下午5:38:29 liushiming
 */
public interface ClientMessageHandler extends EntryExpiredListener<Long, GlobalLog> {

  String processMessage(BeginMessage beginMessage, String clientIp);

  void processMessage(GlobalCommitMessage globalCommitMessage);

  void processMessage(GlobalRollbackMessage globalRollbackMessage);


  public static ClientMessageHandler createClientMessageProcessor(DtsLogDao dtsLogDao,
      DtsServerMessageSender messageSender, HazelcastInstance hazelcatInstance) {

    return new ClientMessageHandler() {
      private final Logger logger = LoggerFactory.getLogger(RmMessageHandler.class);

      private final SyncRmMessagHandler globalResultMessageHandler =
          SyncRmMessagHandler.createSyncGlobalResultProcess(dtsLogDao, messageSender);

      private final IMap<Long, GlobalLog> COMMINTING_GLOBALLOG_CACHE =
          hazelcatInstance.getMap("COMMINTING_GLOBALLOG_CACHE");

      private final IMap<Long, GlobalLog> ROLLBACKING_GLOBALLOG_CACHE =
          hazelcatInstance.getMap("ROLLBACKING_GLOBALLOG_CACHE");

      // 开始一个事务
      @Override
      public String processMessage(BeginMessage beginMessage, String clientIp) {
        GlobalLog globalLog = new GlobalLog();
        globalLog.setState(GlobalLogState.Begin.getValue());
        globalLog.setTimeout(beginMessage.getTimeout());
        globalLog.setClientAppName(clientIp);
        dtsLogDao.insertGlobalLog(globalLog);
        long tranId = globalLog.getTransId();
        String xid = DtsXID.generateXID(tranId);
        return xid;
      }

      // 事务提交
      @Override
      public void processMessage(GlobalCommitMessage globalCommitMessage) {
        Long tranId = globalCommitMessage.getTranId();
        GlobalLog globalLog = dtsLogDao.getGlobalLog(tranId);
        if (globalLog == null) {
          throw new DtsException("transaction doesn't exist.");
        } else {
          switch (GlobalLogState.parse(globalLog.getState())) {
            case Begin:
              if (COMMINTING_GLOBALLOG_CACHE.get(tranId) == null) {
                List<BranchLog> branchLogs = dtsLogDao.getBranchLogs(tranId);
                try {
                  // 设置事务状态为提交中状态
                  globalLog.setState(GlobalLogState.Commiting.getValue());
                  // 缓存提交中状态
                  COMMINTING_GLOBALLOG_CACHE.put(tranId, globalLog, globalLog.getTimeout(),
                      TimeUnit.MILLISECONDS);
                  // 计算事务超时
                  COMMINTING_GLOBALLOG_CACHE.addEntryListener(this, tranId, true);
                  // 通知各个分支开始提交
                  this.syncGlobalCommit(branchLogs, globalLog.getTransId());
                  // 设置事务状态为已提交状态
                  globalLog.setState(GlobalLogState.Committed.getValue());
                  // 清除数据库的事务
                  dtsLogDao.deleteGlobalLog(globalLog.getTransId());
                  // 清除缓存中的事务
                  COMMINTING_GLOBALLOG_CACHE.remove(tranId);
                } catch (Exception e) {
                  logger.error(e.getMessage(), e);
                  globalLog.setState(GlobalLogState.CmmittedFailed.getValue());
                  dtsLogDao.updateGlobalLog(globalLog);
                  throw new DtsException(e, "notify resourcemanager to commit failed");
                }
              }
            case Commiting:
              throw new DtsException("Transaction is commiting!transactionId is:" + tranId);
            default:
              throw new DtsException("Unknown state " + globalLog.getState());
          }

        }
      }

      // 事务回滚
      @Override
      public void processMessage(GlobalRollbackMessage globalRollbackMessage) {
        long tranId = globalRollbackMessage.getTranId();
        GlobalLog globalLog = dtsLogDao.getGlobalLog(tranId);
        if (globalLog == null) {
          throw new DtsException("transaction doesn't exist.");
        } else {
          switch (GlobalLogState.parse(globalLog.getState())) {
            case Begin:
              if (ROLLBACKING_GLOBALLOG_CACHE.get(tranId) == null) {
                List<BranchLog> branchLogs = dtsLogDao.getBranchLogs(tranId);
                try {
                  // 设置事务状态为回滚中状态
                  globalLog.setState(GlobalLogState.Rollbacking.getValue());
                  // 缓存回滚中状态
                  ROLLBACKING_GLOBALLOG_CACHE.put(tranId, globalLog, globalLog.getTimeout(),
                      TimeUnit.MILLISECONDS);
                  ROLLBACKING_GLOBALLOG_CACHE.addEntryListener(this, tranId, true);
                  // 通知各个分支开始回滚
                  this.syncGlobalRollback(branchLogs, globalLog.getTransId());
                  globalLog.setState(GlobalLogState.Rollbacked.getValue());
                  dtsLogDao.deleteGlobalLog(globalLog.getTransId());
                  ROLLBACKING_GLOBALLOG_CACHE.remove(tranId);
                } catch (Exception e) {
                  logger.error(e.getMessage(), e);
                  globalLog.setState(GlobalLogState.RollbackFailed.getValue());
                  dtsLogDao.updateGlobalLog(globalLog);
                  throw new DtsException("notify resourcemanager to commit failed");
                }
              }
            case Rollbacking:
              throw new DtsException("Transaction is robacking!transactionId is:" + tranId);
            default:
              throw new DtsException("Unknown state " + globalLog.getState());
          }
        }
      }

      protected void syncGlobalCommit(List<BranchLog> branchLogs, long transId) {
        for (BranchLog branchLog : branchLogs) {
          String clientAddress = branchLog.getClientIp();
          String clientInfo = branchLog.getClientInfo();
          Long branchId = branchLog.getBranchId();
          BranchCommitMessage branchCommitMessage = new BranchCommitMessage();
          branchCommitMessage.setServerAddr(DtsXID.getSvrAddr());
          branchCommitMessage.setTranId(transId);
          branchCommitMessage.setBranchId(branchId);
          branchCommitMessage.setResourceInfo(branchLog.getClientInfo());
          BranchCommitResultMessage branchCommitResult = null;
          try {
            branchCommitResult = messageSender.invokeSync(clientAddress, clientInfo,
                branchCommitMessage, RemoteConstant.RPC_INVOKE_TIMEOUT);
          } catch (DtsException e) {
            String message =
                "notify " + clientAddress + " commit occur system error,branchId:" + branchId;
            logger.error(message, e);
            throw new DtsException(e, message);
          }
          if (branchCommitResult != null) {
            globalResultMessageHandler.processMessage(clientAddress, branchCommitResult);
          } else {
            throw new DtsException(
                "notify " + clientAddress + " commit response null,branchId:" + branchId);
          }
        }

      }

      protected void syncGlobalRollback(List<BranchLog> branchLogs, long transId) {
        for (int i = 0; i < branchLogs.size(); i++) {
          BranchLog branchLog = branchLogs.get(i);
          Long branchId = branchLog.getBranchId();
          String clientAddress = branchLog.getClientIp();
          String clientInfo = branchLog.getClientInfo();
          BranchRollBackMessage branchRollbackMessage = new BranchRollBackMessage();
          branchRollbackMessage.setServerAddr(DtsXID.getSvrAddr());
          branchRollbackMessage.setTranId(transId);
          branchRollbackMessage.setBranchId(branchId);
          branchRollbackMessage.setResourceInfo(branchLog.getClientInfo());
          BranchRollbackResultMessage branchRollbackResult = null;
          try {
            branchRollbackResult = messageSender.invokeSync(clientAddress, clientInfo,
                branchRollbackMessage, RemoteConstant.RPC_INVOKE_TIMEOUT);
          } catch (DtsException e) {
            String message =
                "notify " + clientAddress + " rollback occur system error,branchId:" + branchId;
            logger.error(message, e);
            throw new DtsException(e, message);
          }
          if (branchRollbackResult != null) {
            globalResultMessageHandler.processMessage(clientAddress, branchRollbackResult);
          } else {
            throw new DtsException(
                "notify " + clientAddress + " rollback response null,branchId:" + branchId);
          }
        }
      }

      // 提交超时或者回滚超时，都触发重新回滚
      @Override
      public void entryExpired(EntryEvent<Long, GlobalLog> event) {
        long tranId = event.getKey();
        GlobalLog globalLog = dtsLogDao.getGlobalLog(tranId);
        List<BranchLog> branchLogs = dtsLogDao.getBranchLogs(tranId);
        try {
          // 通知各个分支开始回滚
          this.syncGlobalRollback(branchLogs, globalLog.getTransId());
          globalLog.setState(GlobalLogState.Rollbacked.getValue());
          dtsLogDao.deleteGlobalLog(globalLog.getTransId());
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
          globalLog.setState(GlobalLogState.RollbackFailed.getValue());
          dtsLogDao.updateGlobalLog(globalLog);
          throw new DtsException("notify resourcemanager to commit failed");
        }

      }

    };
  }

}
