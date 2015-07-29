package com.didapinche.commons.redis;

import org.springframework.beans.factory.InitializingBean;
import redis.clients.jedis.*;

import java.util.List;
import java.util.Random;

/**
 * MasterSlaveRedisPool.java
 * ex:
 *   <bean id="redisPool" class="com.didapinche.commons.redis.MasterSlaveRedisPool">
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


    private JedisPool jedisPool;

    private List<JedisShardInfo> masterShards;
    private List<JedisShardInfo> slaveShards;

    private ShardedJedisPool masterShardedJedisPool;
    private ShardedJedisPool slaveShardedJedisPool;


    public Jedis getResource(){
        return jedisPool.getResource();
    }

    public void returnResourceObject(Jedis jedis){
        jedisPool.returnResourceObject(jedis);
    }

    public ShardedJedis getMasterResource(){
        return masterShardedJedisPool.getResource();
    }
    public void returnMasterResourceObject(ShardedJedis jedis) {
        masterShardedJedisPool.returnResourceObject(jedis);
    }


    public ShardedJedis getSlaveResource(){
        return slaveShardedJedisPool.getResource();
    }

    public void returnSlaveResourceObject(ShardedJedis jedis) {
        slaveShardedJedisPool.returnResourceObject(jedis);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if( masterShards != null ) {
            masterShardedJedisPool = new ShardedJedisPool(jedisPoolConfig,masterShards);
        }

        if( slaveShards != null ) {
            slaveShardedJedisPool = new ShardedJedisPool(jedisPoolConfig,slaveShards);
        }
    }

    public void setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public void setMasterShards(List<JedisShardInfo> masterShards) {
        this.masterShards = masterShards;
    }

    public void setSlaveShards(List<JedisShardInfo> slaveShards) {
        this.slaveShards = slaveShards;
    }

    public void setMasterShardedJedisPool(ShardedJedisPool masterShardedJedisPool) {
        this.masterShardedJedisPool = masterShardedJedisPool;
    }

    public void setSlaveShardedJedisPool(ShardedJedisPool slaveShardedJedisPool) {
        this.slaveShardedJedisPool = slaveShardedJedisPool;
    }

    public void setJedisPoolConfig(JedisPoolConfig jedisPoolConfig) {
        this.jedisPoolConfig = jedisPoolConfig;
    }


    public JedisPoolConfig getJedisPoolConfig() {
        return jedisPoolConfig;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public List<JedisShardInfo> getMasterShards() {
        return masterShards;
    }

    public List<JedisShardInfo> getSlaveShards() {
        return slaveShards;
    }

    public ShardedJedisPool getMasterShardedJedisPool() {
        return masterShardedJedisPool;
    }

    public ShardedJedisPool getSlaveShardedJedisPool() {
        return slaveShardedJedisPool;
    }
}
