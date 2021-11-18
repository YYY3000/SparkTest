package com.yyy.data;

/**
 * ResultSet处理接口
 *
 * @author yinyiyun
 * @date 2018/5/2 10:32
 */

import java.sql.ResultSet;

public interface ResultSetHandler {

    /**
     * 处理sql查询记录
     *
     * @param resultSet 查询结果
     * @throws Exception
     */
    void handle(ResultSet resultSet) throws Exception;

}
