package com.didapinche.commons.redis;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

/**
 * Created by fengbin on 15/7/20.
 */

@ContextConfiguration(locations = {"classpath*:spring.xml"})
public class RedisTest extends RedisTestBase {

    @Test
    public void testRedisTemp(){
        redisClient.hset("abc","abcd","1");


        //System.out.println(redisClient.hget("abc","abcd"));


        redisClient.hincrBy("abc", "abcd", 1);

        System.out.println(redisClient.hget("abc","abcd"));



    }
//
//
//    @Test
//    public void testSentinel(){
//        redisSentinelClient.set("abc","1");
//        redisSentinelClient.incr("abc");
//        System.out.println(redisSentinelClient.get("abc"));
//
//        try {
//            int i= 0;
//            while (true) {
//                Thread.currentThread().sleep(10000l);
//                //System.out.println(redisSentinelClient.getRedisSentinelPool().getMasterResource());
//                //System.out.println(redisSentinelClient.getRedisSentinelPool().getSlaveResource());
//
//                i++;
//                redisSentinelClient.set("abc"+i,String.valueOf(i));
//                redisSentinelClient.incr("abc" + i);
//                System.out.println(redisSentinelClient.get("abc"+i));
//            }
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }


}
