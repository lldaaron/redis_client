package com.didapinche.commons.redis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

/**
 * @author 罗立东 rod
 * @time 15/7/30
 */
@ContextConfiguration(locations = {"classpath*:spring_MasterSlave.xml"})
public class MasterSlaveRedisTest extends RedisTestBase {

    @Autowired
    private MasterSlaveRedisClient client;
}
