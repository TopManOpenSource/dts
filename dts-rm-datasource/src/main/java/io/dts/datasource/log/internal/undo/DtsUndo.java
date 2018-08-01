package io.dts.datasource.log.internal.undo;

import java.util.List;

import io.dts.common.exception.DtsException;
import io.dts.parser.DtsObjectWapper;
import io.dts.parser.struct.RollbackInfor;
import io.dts.parser.struct.TxcField;
import io.dts.parser.struct.TxcLine;
import io.dts.parser.struct.TxcTable;

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

    protected String fieldNamesSerialization(List<TxcField> fields) {
        StringBuilder appender = new StringBuilder();
        boolean bAndFlag = true;
        for (int i = 0; i < fields.size(); i++) {
            TxcField field = fields.get(i);
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

    protected String fieldsValueSerialization(List<TxcField> fields) {
        StringBuilder appender = new StringBuilder();
        boolean bStokFlag = true;
        for (int i = 0; i < fields.size(); i++) {
            TxcField field = fields.get(i);

            if (bStokFlag) {
                bStokFlag = false;
            } else {
                appender.append(", ");
            }

            appender.append(DtsObjectWapper.jsonObjectDeserialize(field.getFieldType(), field.getFieldValue()));
        }

        return appender.toString();
    }

    public abstract List<String> buildRollbackSql();

    protected String fieldsExpressionSerialization(List<TxcField> fields, String stok, String pkname, boolean onlyKey) {
        StringBuilder appender = new StringBuilder();
        boolean bStokFlag = true;
        for (TxcField field : fields) {
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
            appender.append(DtsObjectWapper.jsonObjectDeserialize(field.getFieldType(), field.getFieldValue()));
        }
        return appender.toString();
    }

    protected String linesExpressionSerialization(List<TxcLine> lines, String pkname, boolean onlyKey) {
        StringBuilder appender = new StringBuilder();
        boolean bOrFlag = true;
        for (int index = 0; index < lines.size(); index++) {
            TxcLine line = lines.get(index);

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

    public TxcTable getOriginalValue() {
        return txcUndoLogRollbackInfor.getOriginalValue();
    }

    public TxcTable getPresentValue() {
        return txcUndoLogRollbackInfor.getPresentValue();
    }
}
