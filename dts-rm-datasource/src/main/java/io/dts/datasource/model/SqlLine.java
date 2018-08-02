package io.dts.datasource.model;

import java.util.ArrayList;
import java.util.List;
import com.alibaba.fastjson.annotation.JSONField;

public class SqlLine {
    /**
     * 保存与数据库表对应的一行内容
     */
    private List<SqlField> fields = null;

    @JSONField(serialize = false)
    private SqlTableMeta tableMeta;

    public SqlTableMeta getTableMeta() {
        return tableMeta;
    }

    public void setTableMeta(SqlTableMeta tableMeta) {
        this.tableMeta = tableMeta;
    }

    public SqlLine() {}

    public List<SqlField> getFields() {
        return fields;
    }

    public void addFields(SqlField field) {
        if (fields == null) {
            fields = new ArrayList<SqlField>();
        }
        fields.add(field);
    }

    public void setFields(List<SqlField> fields) {
        this.fields = fields;
    }

    public int getFieldsNum() {
        return fields.size();
    }
}
