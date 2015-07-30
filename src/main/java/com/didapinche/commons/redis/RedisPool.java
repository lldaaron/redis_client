package com.didapinche.commons.redis;

import com.didapinche.commons.redis.sentinel.SentinelActor;
import redis.clients.jedis.*;

/**
 *
 * 线程池接口
 * RedisClientException.java
 * Project: redis client
 *
 * File Created at 2015-7-30 by fengbin
 *
 * Copyright 2015 didapinche.com
 */
public interface RedisPool<T extends JedisCommands> extends SentinelActor{
    /**
     * 初始化redis连接池
     */
    void initPool();

    /**
     * 初始化Master连接池
     */
    void initMasterPool();

    /**
     * 初始化slave连接池
     */
    void initSlavePool();
    /**
     * 是否存在slave
     */
    boolean hasSlave();
    /**
     * 获取Master资源
     */
    <T>T getMasterResource();
    /**
     * 释放Master资源
     */
    void returnMasterResourceObject(T jedis);
    /**
     * 获取Slave资源
     */
    <T>T getSlaveResource();

    /**
     * 释放Master资源
     */
    void returnSlaveResourceObject(T jedis);
}