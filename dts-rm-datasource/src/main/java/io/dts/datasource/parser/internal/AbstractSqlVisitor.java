package io.dts.datasource.parser.internal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;

import io.dts.common.exception.DtsException;
import io.dts.datasource.model.ColumnMeta;
import io.dts.datasource.model.SqlField;
import io.dts.datasource.model.SqlLine;
import io.dts.datasource.model.SqlModel;
import io.dts.datasource.model.SqlTable;
import io.dts.datasource.model.SqlTableMeta;
import io.dts.datasource.model.SqlType;
import io.dts.datasource.parser.SqlVisitor;
import io.dts.datasource.parser.internal.helper.DtsObjectHelper;
import io.dts.datasource.parser.internal.helper.DtsTableMetaHelper;

public abstract class AbstractSqlVisitor extends MySqlOutputVisitor implements SqlVisitor {

    private String selectSql = null;

    private String whereCondition = null;

    private SqlTableMeta tableMeta = null;// table语法树

    protected SqlModel node;
    private final SqlTable tableOriginalValue = new SqlTable(); // 保存SQL前置镜像
    private final SqlTable tablePresentValue = new SqlTable(); // 保存SQL后置镜像

    protected Connection connection;

    protected String tableName;

    protected String tableNameAlias;

    public AbstractSqlVisitor(SqlModel node, List<Object> parameterSet) {
        super(new StringBuilder());
        this.node = node;
        super.setParameters(parameterSet);
        node.getSQLStatement().accept(this);
    }

    @Override
    public void setConnection(final Connection connection) {
        this.connection = connection;
    }

    @Override
    public SqlTableMeta buildTableMeta() throws SQLException {
        if (tableMeta != null) {
            return tableMeta;
        }
        try {
            String tablename = getTableName();
            tableMeta = DtsTableMetaHelper.getTableMeta(connection, tablename);
            tableMeta.setAlias(tableNameAlias);
        } catch (Exception e) {
            throw new DtsException(e, "getTableMeta error");
        }

        return tableMeta;
    }

    @Override
    public SqlTable getTableOriginalValue() {
        return tableOriginalValue;
    }

    @Override
    public SqlTable getTablePresentValue() {
        return tablePresentValue;
    }

    @Override
    public SqlTableMeta getTableMeta() {
        return tableMeta;
    }

    @Override
    public String getFullSql() {
        return getAppender().toString();
    }

    @Override
    public String getSelectSql() {
        if (selectSql == null) {
            selectSql = parseSelectSql();
        }
        return selectSql;
    }

    public String parseSelectSql() {
        StringBuilder appendable = new StringBuilder();
        appendable.append("SELECT ");
        appendable.append(printColumns());
        appendable.append(" FROM ");
        appendable.append(getTableName());
        appendable.append(" ");
        if (getTableNameAlias() != null) {
            appendable.append(getTableNameAlias());
        }
        return appendable.toString();
    }

    public String printColumns() {
        StringBuilder appender = new StringBuilder();

        Collection<ColumnMeta> list = (Collection<ColumnMeta>)getTableMeta().getAllColumns().values();

        boolean isFst = true;
        for (Object obj : list) {
            if (isFst) {
                isFst = false;
            } else if (obj instanceof ColumnMeta) {
                appender.append(",");
            }
            if (getTableNameAlias() != null) {
                appender.append(getTableNameAlias());
                appender.append(".");
            }

            appender.append(((ColumnMeta)obj).getColumnName());
        }

        return appender.toString();
    }

    public String getWhereCondition(Statement st) {
        if (whereCondition == null) {
            whereCondition = " WHERE " + parseWhereCondition(st);
        }
        return whereCondition;
    }

    @Override
    public String getWhereCondition(SqlTable table) {
        StringBuilder appender = new StringBuilder();

        Map<String, ColumnMeta> tableKeys = getTableMeta().getPrimaryKeyMap();
        if (tableKeys.size() <= 0) {
            throw new DtsException(
                "table[" + getTableMeta().getTableName() + "] should has prikey, contact DBA please.");
        }

        List<SqlLine> lines = table.getLines();
        if (lines.size() > 0) {
            appender.append(" WHERE ");
        }

        boolean bOrFlag = true;
        for (int i = 0; i < lines.size(); i++) {
            SqlLine line = lines.get(i);
            List<SqlField> fields = line.getFields();
            if (fields == null) {
                continue;
            }

            if (bOrFlag) {
                bOrFlag = false;
            } else {
                appender.append(" OR ");
            }

            printKeyList(tableKeys, fields, appender);
        }

        return appender.toString();
    }

    void printKeyList(Map<String, ColumnMeta> tableKeys, List<SqlField> fields, StringBuilder appender) {
        boolean bAndFlag = true;
        for (int i = 0; i < fields.size(); i++) {
            SqlField field = fields.get(i);
            if (tableKeys.containsKey(field.getFieldName().toUpperCase())) {
                if (bAndFlag) {
                    bAndFlag = false;
                } else {
                    appender.append(" AND ");
                }
                appender.append(field.getFieldName());
                appender.append("=");
                DtsObjectHelper.appendParamMarkerObject(field.getFieldValue(), appender);
            }
        }
    }

    protected StringBuffer parseWhereCondition(final SQLExpr where) {
        StringBuffer out = new StringBuffer();
        SQLASTOutputVisitor sqlastOutputVisitor =
            SQLUtils.createFormatOutputVisitor(out, null, node.getDatabaseType().getDruidSqlType());
        sqlastOutputVisitor.setParameters(getParameters());
        where.accept(sqlastOutputVisitor);
        return out;
    }

    protected abstract String parseWhereCondition(final Statement st);

    @Override
    public SqlType getSqlType() {
        return node.getType();
    }

    @Override
    public SqlModel getSQLStatement() {
        return this.node;
    }

    @Override
    public String getInputSql() {
        return node.getSql();
    }

    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }

    public void setTableNameAlias(final String tableNameAlias) {
        this.tableNameAlias = tableNameAlias;
    }

    public String getTableNameAlias() {
        return tableNameAlias;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    private SqlLine addLine(ResultSet rs, ResultSetMetaData rsmd, int column) throws SQLException {
        List<SqlField> fields = new ArrayList<SqlField>(column);
        for (int i = 1; i <= column; i++) {
            SqlField field = new SqlField();
            field.setFieldName(rsmd.getColumnName(i));
            field.setFieldType(rsmd.getColumnType(i));
            if (rsmd.getColumnTypeName(i).equals("TINYINT")) {
                field.setFieldValue(rs.getInt(i));
            } else {
                field.setFieldValue(rs.getObject(i));
            }

            fields.add(field);
        }

        SqlLine line = new SqlLine();
        line.setTableMeta(getTableMeta());
        line.setFields(fields);
        return line;
    }

    public List<SqlLine> addLines(String sql) throws SQLException {
        List<SqlLine> txcLines = new ArrayList<>();
        Statement st = null;
        ResultSet rs = null;
        try {
            st = connection.createStatement();
            rs = st.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            int column = rsmd.getColumnCount();

            while (rs.next()) {
                txcLines.add(addLine(rs, rsmd, column));
            }
            return txcLines;
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }
        }
    }

}
