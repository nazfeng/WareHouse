package com.atguigu;

import redis.clients.jedis.Jedis;

/**
 * @author NaZFeng
 * @create 2020-07-13 18:57
 */
public class jedisTest {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("hadoop102", 6379);

        String ping = jedis.ping();
        jedis.set("salary","8000");

//        System.out.println(ping);

        jedis.close();
    }
}
