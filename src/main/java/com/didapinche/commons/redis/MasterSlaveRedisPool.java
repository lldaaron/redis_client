package com.didapinche.commons.redis;

import org.springframework.beans.factory.InitializingBean;
import redis.clients.jedis.*;

import java.util.List;

/**
 * MuiltRedisPool.java
 * ex:
 *   <bean id="redisPool" class="com.didapinche.commons.redis.MuiltRedisPool">
 *      <property name="jedisPoolConfig" ref="jedisPoolConfig"></property>
 *      <property name="masterShards">
 *           <list>
 *               <bean class="redis.clients.jedis.JedisShardInfo">
 *                   <constructor-arg name="host" value="127.0.0.1"></constructor-arg>
 *                   <constructor-arg name="port" value="6380"></constructor-arg>
 *               </bean>
 *           </list>
 *       </property>
 *       <property name="slaveShards">
 *           <list>
 *               <bean class="redis.clients.jedis.JedisShardInfo">
 *                   <constructor-arg name="host" value="127.0.0.1"></constructor-arg>
 *                   <constructor-arg name="port" value="6390"></constructor-arg>
 *               </bean>
 *           </list>
 *       </property>
 *   </bean>
 * Project: redis client
 *
 * File Created at 2015-7-28 by fengbin
 *
 * Copyright 2015 didapinche.com
 */
public class MasterSlaveRedisPool implements InitializingBean{
    private JedisPoolConfig jedisPoolConfig;


    private JedisPool masterJedisPool;
    private JedisPool slaveJedisPool;


    public Jedis getMasterResource(){
        return masterJedisPool.getResource();
    }
    public void returnMasterResourceObject(Jedis jedis) {
        masterJedisPool.returnResourceObject(jedis);
    }


    public Jedis getSlaveResource(){
        return slaveJedisPool.getResource();
    }

    public void returnSlaveResourceObject(Jedis jedis) {
        slaveJedisPool.returnResourceObject(jedis);
    }

    @Override
    public void afterPropertiesSet() throws Exception {

    }


    public void setJedisPoolConfig(JedisPoolConfig jedisPoolConfig) {
        this.jedisPoolConfig = jedisPoolConfig;
    }


    public JedisPoolConfig getJedisPoolConfig() {
        return jedisPoolConfig;
    }

    public JedisPool getMasterJedisPool() {
        return masterJedisPool;
    }

    public void setMasterJedisPool(JedisPool masterJedisPool) {
        this.masterJedisPool = masterJedisPool;
    }

    public JedisPool getSlaveJedisPool() {
        return slaveJedisPool;
    }

    public void setSlaveJedisPool(JedisPool slaveJedisPool) {
        this.slaveJedisPool = slaveJedisPool;
    }
}
