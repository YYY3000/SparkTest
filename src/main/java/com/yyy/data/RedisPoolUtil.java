package com.yyy.data;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author yinyiyun
 * @date 2018/8/6 10:21
 */
public class RedisPoolUtil {

    private JedisPool pool;

    /**
     * 连接 redis 等待时间
     */
    private static int timeOut = 10000;

    /**
     * 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException
     */
    private static int maxWait = 10000;

    /**
     * 构造器
     *
     * @param address  地址
     * @param port     端口
     * @param password 密码
     */
    public RedisPoolUtil(String address, String port, String password) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxWaitMillis(maxWait);
        // 是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的
        config.setTestOnBorrow(true);
        this.pool = new JedisPool(config, address, Integer.valueOf(port), timeOut, password);
    }

    public Jedis getJedis() {
        return pool.getResource();
    }

}
