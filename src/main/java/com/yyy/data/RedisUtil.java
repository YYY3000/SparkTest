package com.yyy.data;

import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

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

    public static void main(String[] args) {
        RedisUtil redisUtil = new RedisUtil("hadoop-dn01", "6379", "mastercom");
        Jedis jedis = redisUtil.getJedis();
        Map<String, String> map = jedis.hgetAll("sys_alarmtype_id");
        for (Map.Entry<String, String> entry : map.entrySet()) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }
        Map<String, String> m = new HashMap<>();
        m.put("yyy", "yyy");
        m.put("kkk", "kkk");
        m.put("ttt", "ttt");
        m.put("www", "www");
        m.put("ppp", "ppp");
        jedis.hset("map_test".getBytes(), "name".getBytes(), m.toString().getBytes());
    }
}
