package com.didapinche.commons.redis;

import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

/**
 * Project: redis client
 *
 * File Created at 2015-7-29 by fengbin
 *
 * Copyright 2015 didapinche.com
 */
@ContextConfiguration(locations = {"classpath*:spring.xml"})
public class RedisTestBase extends AbstractJUnit4SpringContextTests {


    @Autowired
    protected RedisClient masterSlaveRedisClient;

    @Autowired
    protected RedisClient muiltRedisClient;


    @Before
    public void setup(){

    }


}
