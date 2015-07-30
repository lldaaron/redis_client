package com.didapinche.commons.redis;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

/**
 * Created by fengbin on 15/7/20.
 */

@ContextConfiguration(locations = {"classpath*:spring.xml"})
public class MultiRedisTest extends RedisTestBase {

    @Test
    public void testMasterSlave(){

    }

    @Test
    public void testMuilt(){
        muiltRedisClient.set("abc","1");
        muiltRedisClient.incr("abc");
        System.out.println(muiltRedisClient.get("abc"));

        try {
            int i= 0;

            while (true) {
                Thread.currentThread().sleep(10000l);

                i++;
                muiltRedisClient.set("abc"+i,String.valueOf(i));
                muiltRedisClient.incr("abc" + i);

                System.out.println(muiltRedisClient.get("abc"+i));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
