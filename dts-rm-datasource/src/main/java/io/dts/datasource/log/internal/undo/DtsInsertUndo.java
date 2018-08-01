package io.dts.datasource.log.internal.undo;

import java.util.List;
import com.google.common.collect.Lists;
import io.dts.parser.struct.RollbackInfor;

public class DtsInsertUndo extends DtsUndo {

    public DtsInsertUndo(RollbackInfor txcUndoLogRollbackInfor) {
        super(txcUndoLogRollbackInfor);
    }

    @Override
    public List<String> buildRollbackSql() {
        // 可一并删除，减少SQL数
        List<String> sqls = Lists.newArrayList();

        if (getPresentValue().getLinesNum() > 0) {
            String tableName = getPresentValue().getTableMeta().getTableName();
            String pkName = getPresentValue().getTableMeta().getPkName();

            StringBuilder sqlAppender = new StringBuilder();
            sqlAppender.append("DELETE FROM ");
            sqlAppender.append(tableName);
            sqlAppender.append(" WHERE ");
            sqlAppender.append(linesExpressionSerialization(getPresentValue().getLines(), pkName, true));

            sqls.add(sqlAppender.toString());
        }
        return sqls;
    }
}
