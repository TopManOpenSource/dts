package io.dts.datasource.log.internal.undo;

import java.util.List;

import io.dts.common.exception.DtsException;
import io.dts.datasource.model.RollbackInfor;
import io.dts.datasource.model.SqlField;
import io.dts.datasource.model.SqlLine;
import io.dts.datasource.model.SqlTable;
import io.dts.datasource.util.DtsObjectUtil;

public abstract class DtsUndo {

    private final RollbackInfor txcUndoLogRollbackInfor;

    public static DtsUndo createDtsundo(RollbackInfor undoLog) {
        DtsUndo undo = null;
        switch (undoLog.getSqlType()) {
            case DELETE:
                undo = new DtsDeleteUndo(undoLog);
                break;
            case INSERT:
                undo = new DtsInsertUndo(undoLog);
                break;
            case UPDATE:
                undo = new DtsUpdateUndo(undoLog);
                break;
            default:
                throw new DtsException("sqltype error:" + undoLog.getSqlType());
        }

        return undo;
    }

    protected DtsUndo(RollbackInfor txcUndoLogRollbackInfor) {
        this.txcUndoLogRollbackInfor = txcUndoLogRollbackInfor;
    }

    protected String fieldNamesSerialization(List<SqlField> fields) {
        StringBuilder appender = new StringBuilder();
        boolean bAndFlag = true;
        for (int i = 0; i < fields.size(); i++) {
            SqlField field = fields.get(i);
            if (bAndFlag) {
                bAndFlag = false;
            } else {
                appender.append(", ");
            }
            appender.append('`');
            appender.append(field.getFieldName());
            appender.append('`');
        }

        return appender.toString();
    }

    protected String fieldsValueSerialization(List<SqlField> fields) {
        StringBuilder appender = new StringBuilder();
        boolean bStokFlag = true;
        for (int i = 0; i < fields.size(); i++) {
            SqlField field = fields.get(i);

            if (bStokFlag) {
                bStokFlag = false;
            } else {
                appender.append(", ");
            }

            appender.append(DtsObjectUtil.jsonObjectDeserialize(field.getFieldType(), field.getFieldValue()));
        }

        return appender.toString();
    }

    public abstract List<String> buildRollbackSql();

    protected String fieldsExpressionSerialization(List<SqlField> fields, String stok, String pkname, boolean onlyKey) {
        StringBuilder appender = new StringBuilder();
        boolean bStokFlag = true;
        for (SqlField field : fields) {
            if (onlyKey && field.isKey(pkname) == false) {
                continue;
            }
            if (bStokFlag) {
                bStokFlag = false;
            } else {
                appender.append(" " + stok + " ");
            }
            appender.append('`');
            appender.append(field.getFieldName());
            appender.append('`');
            appender.append(" = ");
            appender.append(DtsObjectUtil.jsonObjectDeserialize(field.getFieldType(), field.getFieldValue()));
        }
        return appender.toString();
    }

    protected String linesExpressionSerialization(List<SqlLine> lines, String pkname, boolean onlyKey) {
        StringBuilder appender = new StringBuilder();
        boolean bOrFlag = true;
        for (int index = 0; index < lines.size(); index++) {
            SqlLine line = lines.get(index);

            if (bOrFlag) {
                bOrFlag = false;
            } else {
                appender.append(" OR ");
            }

            appender.append(fieldsExpressionSerialization(line.getFields(), "AND", pkname, onlyKey));
        }

        return appender.toString();
    }

    public RollbackInfor getTxcUndoLogRollbackInfor() {
        return txcUndoLogRollbackInfor;
    }

    public SqlTable getOriginalValue() {
        return txcUndoLogRollbackInfor.getOriginalValue();
    }

    public SqlTable getPresentValue() {
        return txcUndoLogRollbackInfor.getPresentValue();
    }
}
