
package io.dts.datasource.parser;

import java.util.HashMap;
import java.util.Map;

import io.dts.datasource.model.DatabaseType;
import io.dts.datasource.parser.internal.DeleteVisitor;
import io.dts.datasource.parser.internal.InsertVisitor;
import io.dts.datasource.parser.internal.SelectVisitor;
import io.dts.datasource.parser.internal.UpdateVisitor;

public final class SQLVisitorRegistry {

    private static final Map<DatabaseType, Class<? extends SqlVisitor>> SELECT_REGISTRY =
        new HashMap<>(DatabaseType.values().length);

    private static final Map<DatabaseType, Class<? extends SqlVisitor>> INSERT_REGISTRY =
        new HashMap<>(DatabaseType.values().length);

    private static final Map<DatabaseType, Class<? extends SqlVisitor>> UPDATE_REGISTRY =
        new HashMap<>(DatabaseType.values().length);

    private static final Map<DatabaseType, Class<? extends SqlVisitor>> DELETE_REGISTRY =
        new HashMap<>(DatabaseType.values().length);

    static {
        registerSelectVistor();
        registerInsertVistor();
        registerUpdateVistor();
        registerDeleteVistor();
    }

    private static void registerSelectVistor() {
        SELECT_REGISTRY.put(DatabaseType.H2, SelectVisitor.class);
        SELECT_REGISTRY.put(DatabaseType.MySQL, SelectVisitor.class);
        SELECT_REGISTRY.put(DatabaseType.Oracle, SelectVisitor.class);
        SELECT_REGISTRY.put(DatabaseType.SQLServer, SelectVisitor.class);
        SELECT_REGISTRY.put(DatabaseType.DB2, SelectVisitor.class);
        SELECT_REGISTRY.put(DatabaseType.PostgreSQL, SelectVisitor.class);
    }

    private static void registerInsertVistor() {
        INSERT_REGISTRY.put(DatabaseType.H2, InsertVisitor.class);
        INSERT_REGISTRY.put(DatabaseType.MySQL, InsertVisitor.class);
        INSERT_REGISTRY.put(DatabaseType.Oracle, InsertVisitor.class);
        INSERT_REGISTRY.put(DatabaseType.SQLServer, InsertVisitor.class);
        INSERT_REGISTRY.put(DatabaseType.DB2, InsertVisitor.class);
        INSERT_REGISTRY.put(DatabaseType.PostgreSQL, InsertVisitor.class);
    }

    private static void registerUpdateVistor() {
        UPDATE_REGISTRY.put(DatabaseType.H2, UpdateVisitor.class);
        UPDATE_REGISTRY.put(DatabaseType.MySQL, UpdateVisitor.class);
        INSERT_REGISTRY.put(DatabaseType.Oracle, UpdateVisitor.class);
        INSERT_REGISTRY.put(DatabaseType.SQLServer, UpdateVisitor.class);
        INSERT_REGISTRY.put(DatabaseType.DB2, UpdateVisitor.class);
        INSERT_REGISTRY.put(DatabaseType.PostgreSQL, UpdateVisitor.class);
    }

    private static void registerDeleteVistor() {
        DELETE_REGISTRY.put(DatabaseType.H2, DeleteVisitor.class);
        DELETE_REGISTRY.put(DatabaseType.MySQL, DeleteVisitor.class);
        INSERT_REGISTRY.put(DatabaseType.Oracle, DeleteVisitor.class);
        INSERT_REGISTRY.put(DatabaseType.SQLServer, DeleteVisitor.class);
        INSERT_REGISTRY.put(DatabaseType.DB2, DeleteVisitor.class);
        INSERT_REGISTRY.put(DatabaseType.PostgreSQL, DeleteVisitor.class);
    }

    /**
     * 获取SELECT访问器.
     * 
     * @param databaseType 数据库类型
     * @return SELECT访问器
     */
    public static Class<? extends SqlVisitor> getSelectVistor(final DatabaseType databaseType) {
        return getVistor(databaseType, SELECT_REGISTRY);
    }

    /**
     * 获取INSERT访问器.
     * 
     * @param databaseType 数据库类型
     * @return INSERT访问器
     */
    public static Class<? extends SqlVisitor> getInsertVistor(final DatabaseType databaseType) {
        return getVistor(databaseType, INSERT_REGISTRY);
    }

    /**
     * 获取UPDATE访问器.
     * 
     * @param databaseType 数据库类型
     * @return UPDATE访问器
     */
    public static Class<? extends SqlVisitor> getUpdateVistor(final DatabaseType databaseType) {
        return getVistor(databaseType, UPDATE_REGISTRY);
    }

    /**
     * 获取DELETE访问器.
     * 
     * @param databaseType 数据库类型
     * @return DELETE访问器
     */
    public static Class<? extends SqlVisitor> getDeleteVistor(final DatabaseType databaseType) {
        return getVistor(databaseType, DELETE_REGISTRY);
    }

    private static Class<? extends SqlVisitor> getVistor(final DatabaseType databaseType,
        final Map<DatabaseType, Class<? extends SqlVisitor>> registry) {
        if (!registry.containsKey(databaseType)) {
            throw new UnsupportedOperationException(databaseType.name());
        }
        return registry.get(databaseType);
    }
}
