package com.yyy.data;

import java.sql.*;

/**
 * 源生jdbc数据库操作工具类
 *
 * @author yinyiyun
 * @date 2018/5/2 10:32
 */
public class DatabaseClient {

    private String driver;

    private String url;

    private String user;

    private String password;

    public DatabaseClient(String driver, String url, String user, String password) {
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    /**
     * 执行查询操作
     *
     * @param sql     sql
     * @param handler 结果处理
     * @throws Exception
     */
    public void executeQuery(String sql, ResultSetHandler handler, Object... params) throws Exception {
        try (Connection connection = getConnection(this.driver, this.url, this.user, this.password);
             PreparedStatement statement = getStatement(connection, sql, params);
             ResultSet rs = statement.executeQuery();) {
            handler.handle(rs);
        }
    }

    /**
     * 执行更新操作
     *
     * @param sql sql
     * @throws Exception
     */
    public void executeNonQuery(String sql, Object... params) throws Exception {
        try (Connection connection = getConnection(this.driver, this.url, this.user, this.password);
             PreparedStatement statement = getStatement(connection, sql, params);) {
            statement.executeUpdate();
        }
    }

    /**
     * 获取 Mysql 的 connection
     *
     * @param driver   驱动类
     * @param url      数据库连接的url
     * @param user     user
     * @param password 密码
     * @return
     */
    private static Connection getConnection(String driver, String url, String user, String password) throws
            Exception {
        Class.forName(driver);
        return DriverManager.getConnection(url, user, password);
    }

    /**
     * 根据 connection 和 sql 获取 statement
     *
     * @param connection 数据库连接
     * @param sql        执行sql
     * @param params     参数
     * @return
     * @throws SQLException
     */
    private static PreparedStatement getStatement(Connection connection, String sql, Object... params) throws
            SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i < params.length; i++) {
            statement.setObject(i + 1, params[i]);
        }
        return statement;
    }

}
