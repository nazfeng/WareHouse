package com.atguigu;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author NaZFeng
 * @create 2020-07-13 19:19
 */
public class jedisPoolTest {
    public static void main(String[] args) {
        JedisPool pool = new JedisPool("hadoop102", 6379);

        Jedis jedis = pool.getResource();

        String ping = jedis.ping();
        jedis.rpush("animal","bird","pig","panda");
        System.out.println(jedis.lrange("animal",0,-1));
        System.out.println(ping);

        pool.close();
    }
}
