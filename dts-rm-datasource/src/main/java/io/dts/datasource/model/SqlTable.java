package io.dts.datasource.model;

import java.util.ArrayList;
import java.util.List;
import com.alibaba.fastjson.annotation.JSONField;
import io.dts.common.exception.DtsException;

public class SqlTable {
    @JSONField(serialize = false)
    private SqlTableMeta tableMeta;
    private String schemaName; // 实例名
    private String tableName; // 表名
    private String alias; // 别名
    private List<SqlLine> lines = new ArrayList<SqlLine>();

    public SqlTable() {}

    public int getLinesNum() {
        return lines.size();
    }

    public List<SqlLine> getLines() {
        return lines;
    }

    public void setLines(List<SqlLine> lineList) {
        this.lines = lineList;
    }

    public void addLine(SqlLine line) {
        lines.add(line);

        if (lines.size() > 1000) {
            throw new DtsException("one sql operated too much lines");
        }
    }

    @SuppressWarnings("serial")
    public List<SqlField> pkRows() {
        final String pkName = getTableMeta().getPkName();
        return new ArrayList<SqlField>() {
            {
                for (SqlLine line : lines) {
                    List<SqlField> fields = line.getFields();
                    for (SqlField field : fields) {
                        if (field.getFieldName().equalsIgnoreCase(pkName)) {
                            add(field);
                            break;
                        }
                    }
                }
            }
        };
    }

    public SqlTableMeta getTableMeta() {
        if (tableMeta == null) {
            throw new DtsException("should set table meta when table data init");
        }
        return tableMeta;
    }

    public void setTableMeta(SqlTableMeta tableMeta) {
        this.tableMeta = tableMeta;
    }

    public String toString() {
        StringBuilder appender = new StringBuilder();
        for (int i = 0; i < lines.size(); i++) {
            List<SqlField> line = lines.get(i).getFields();
            for (SqlField obj : line) {
                appender.append(obj.getFieldValue());
            }
        }
        return appender.toString();
    }

    public String toStringWithEndl() {
        StringBuilder appender = new StringBuilder();
        for (int i = 0; i < lines.size(); i++) {
            List<SqlField> line = lines.get(i).getFields();
            for (SqlField obj : line) {
                appender.append(obj.getFieldValue()).append(":");
            }
            appender.append("\n");
        }
        return appender.toString();
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
