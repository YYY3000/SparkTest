package com.yyy.data;

import redis.clients.jedis.Jedis;

/**
 * @author yinyiyun
 * @date 2018/8/6 10:21
 */
public class RedisUtil {

    private Jedis jedis;

    /**
     * 构造器
     *
     * @param address  地址
     * @param port     端口
     * @param password 密码
     */
    public RedisUtil(String address, String port, String password) {
        this.jedis = new Jedis(address, Integer.valueOf(port));
        this.jedis.auth(password);
    }

    public Jedis getJedis() {
        return jedis;
    }

}
