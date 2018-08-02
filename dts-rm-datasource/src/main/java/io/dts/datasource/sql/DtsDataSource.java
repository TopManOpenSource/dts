package io.dts.datasource.sql;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import io.dts.datasource.DataSourceHolder;
import io.dts.datasource.DataSourceResourceManager;
import io.dts.datasource.model.DatabaseType;
import io.dts.datasource.sql.internal.DataSourceAdaper;
import io.dts.resourcemanager.ResourceManager;

public class DtsDataSource extends DataSourceAdaper {

    private DataSource dataSource;

    private String dbName;

    private ResourceManager resourceManager;

    public DtsDataSource(final DataSource dataSource, String dbName) {
        this.dataSource = dataSource;
        this.dbName = dbName;
        DataSourceHolder.registerDataSource(dbName, dataSource);
    }

    public void setResourceManager(final ResourceManager resourceManager) {
        this.resourceManager = resourceManager;
    }

    public DatabaseType getDatabaseType() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            return DatabaseType.valueFrom(connection.getMetaData().getDatabaseProductName());
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return new DtsConnection(this, dataSource.getConnection());
    }

    @Override
    public Connection getConnection(final String username, final String password) throws SQLException {
        return new DtsConnection(this, dataSource.getConnection(username, password));
    }

    public DataSource getRawDataSource() {
        return dataSource;
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    public ResourceManager getResourceManager() {
        if (resourceManager == null) {
            resourceManager = DataSourceResourceManager.newResourceManager();
        }
        return resourceManager;
    }

}
