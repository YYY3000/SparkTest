package com.mastercom.data;

import java.text.MessageFormat;

/**
 * mysql读写工具类
 *
 * @author yinyiyun
 * @date 2018/6/20 10:25
 */
public class MysqlUtil extends DatabaseUtil {

    private static String driver = "com.mysql.cj.jdbc.Driver";

    public MysqlUtil(String serverName, String port, String dbName, String user, String password) {
        super(serverName, port, dbName, user, password);
    }

    @Override
    protected String getUrl(String serverName, String port, String dbName) {
        return MessageFormat.format("jdbc:mysql://{0}:{1}/{2}?serverTimezone=UTC", serverName, port, dbName);
    }

    @Override
    public String getDriver() {
        return driver;
    }
}
