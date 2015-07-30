package com.didapinche.commons.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import redis.clients.jedis.*;

import java.util.*;

/**
 * RedisSentinelPool.java
 * ex:
 *<bean id ="redisSentinelPool" class="com.didapinche.commons.redis.RedisSentinelPool">
 *    <property name="jedisPoolConfig" ref="jedisPoolConfig"></property>
 *
 *    <property name="sentinelShards">
 *        <map >
 *            <entry key="master1">
 *                <bean class="com.didapinche.commons.redis.sentinel.SentinelInfo">
 *                    <property name="sentinels">
 *                        <set>
 *                            <value>127.0.0.1:26379</value>
 *                            <value>127.0.0.1:26380</value>
 *                            <value>127.0.0.1:26381</value>
 *                        </set>
 *                        </property>
 *                        <property name="weight" value="1"></property>
 *                        <property name="password" value="" ></property>
 *                </bean>
 *            </entry>
 *
 *            <entry key="master2">
 *                <bean class="com.didapinche.commons.redis.sentinel.SentinelInfo">
 *                    <property name="sentinels">
 *                        <set>
 *                            <value>127.0.0.1:26379</value>
 *                            <value>127.0.0.1:26380</value>
 *                            <value>127.0.0.1:26381</value>
 *                        </set>
 *                    </property>
 *                    <property name="weight" value="1"></property>
 *                    <property name="password" value="" ></property>
 *                </bean>
 *            </entry>
 *        </map>
 *    </property>
 *</bean>
 *
 * Project: redis client
 *
 * File Created at 2015-7-28 by fengbin
 *
 * Copyright 2015 didapinche.com
 */
public class MultiRedisPool implements RedisPool<ShardedJedis>, InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(MultiRedisPool.class);
    //记录当前线程使用的线程池，方便释放资源
    private ThreadLocal<ShardedJedisPool> slavePoolTh = new ThreadLocal();


    private JedisPoolConfig jedisPoolConfig;

    //主从配置信息
    private Map<String,JedisShardInfo> masterShards = new HashMap<>();
    private Map<String,List<JedisShardInfo>> multiSlaveShards = new HashMap();

    //直接使用ShardedJedisPool做hash
    private ShardedJedisPool masterShardedJedisPool;
    private List<ShardedJedisPool> slaveShardedJedisPools = new ArrayList<>();


    @Override
    public void afterPropertiesSet() throws Exception {
        initPool();
    }


    @Override
    public boolean hasSlave(){
        return (slaveShardedJedisPools != null && slaveShardedJedisPools.size() >0);
    }

    @Override
    public ShardedJedis getMasterResource(){
        return masterShardedJedisPool.getResource();
    }

    @Override
    public void returnMasterResourceObject(ShardedJedis jedis) {
        masterShardedJedisPool.returnResourceObject(jedis);
    }

    @Override
    public ShardedJedis getSlaveResource(){

        int index = new Random().nextInt(slaveShardedJedisPools.size());
        ShardedJedisPool pool = slaveShardedJedisPools.get(index);
        slavePoolTh.set(pool);

        return pool.getResource();
    }


    @Override
    public void returnSlaveResourceObject(ShardedJedis jedis) {
        slavePoolTh.get().returnResourceObject(jedis);
    }

    @Override
    public  void initPool(){
        initMasterPool();

        initSlavePool();
    }



    @Override
    public void initMasterPool() {
        initMasterPool(new ArrayList<JedisShardInfo>(masterShards.values()));
    }

    private void initMasterPool(List<JedisShardInfo> masterShards) {
        if( masterShards != null && masterShards.size() > 0) {
            masterShardedJedisPool = new ShardedJedisPool(jedisPoolConfig,masterShards);
        }
    }


    @Override
    public  void initSlavePool(){
        initSlavePool(multiSlaveShards);
    }


    private void initSlavePool(Map<String,List<JedisShardInfo>> multiSlaveShards) {
        if( multiSlaveShards != null && multiSlaveShards.size() > 0) {
            for(ShardedJedisPool pool:slaveShardedJedisPools){
                pool.destroy();
            }

            slaveShardedJedisPools.clear();

            int i = 0;
            while (true) {
                List<JedisShardInfo> slaveShards = new ArrayList<JedisShardInfo>();
                for(String masterName:multiSlaveShards.keySet()){
                    List<JedisShardInfo> shardInfos = multiSlaveShards.get(masterName);

                    if(i >= shardInfos.size() ) return;

                    JedisShardInfo shardInfo = shardInfos.get(i);
                    slaveShards.add(shardInfo);

                }

                slaveShardedJedisPools.add(new ShardedJedisPool(jedisPoolConfig,slaveShards));
                i++;
            }

        }
    }






    @Override
    public void buildMasterSlaveInfo(String masterName, HostAndPort masterInfo, List<Map<String, String>> slaveInfos) {
        buildMasterShardInfos(masterName,masterInfo);

        List<JedisShardInfo> jedisShardInfos= new ArrayList<>();

        for(Map<String,String>slaveInfo : slaveInfos) {

            String host = slaveInfo.get("ip");
            String port = slaveInfo.get("port");

            JedisShardInfo slaveShardInfo = new JedisShardInfo(host,Integer.parseInt(port));

            jedisShardInfos.add(slaveShardInfo);
        }

        multiSlaveShards.put(masterName,jedisShardInfos);
        initPool();
    }

    @Override
    public void switchMaster(String masterName, HostAndPort masterInfo){

        buildMasterShardInfos(masterName, masterInfo);
        initMasterPool(new ArrayList<JedisShardInfo>(masterShards.values()));

    }

    @Override
    public void sdownSlave(String masterName,HostAndPort hostAndPort){
        List<JedisShardInfo> slaveInfos = multiSlaveShards.get(masterName);

        for(JedisShardInfo shardInfo:slaveInfos ){
            if(shardInfo.getHost().equals(hostAndPort.getHost()) && shardInfo.getPort() == hostAndPort.getPort()){
                slaveInfos.remove(shardInfo);
                break;
            }
        }

        initSlavePool();
    }



    // 对应sentinel －sdown
    @Override
    public void nsdownSlave(String masterName,HostAndPort hostAndPort){
        List<JedisShardInfo> slaveInfos = multiSlaveShards.get(masterName);

        for(JedisShardInfo shardInfo:slaveInfos ){
            if(shardInfo.getHost().equals(hostAndPort.getHost()) && shardInfo.getPort() == hostAndPort.getPort()){
                return;
            }
        }

        slaveInfos.add(new JedisShardInfo(hostAndPort.getHost(),hostAndPort.getPort()));

        initSlavePool();
    }


    public void buildMasterShardInfos(String masterName, HostAndPort masterInfo){
        JedisShardInfo masterShardInfo = new JedisShardInfo(masterInfo.getHost(),masterInfo.getPort());

        masterShards.put(masterName,masterShardInfo);
    }






    public void setJedisPoolConfig(JedisPoolConfig jedisPoolConfig) {
        this.jedisPoolConfig = jedisPoolConfig;
    }

    public Map<String, JedisShardInfo> getMasterShards() {
        return masterShards;
    }

    public void setMasterShards(Map<String, JedisShardInfo> masterShards) {
        this.masterShards = masterShards;
    }

    public Map<String, List<JedisShardInfo>> getMultiSlaveShards() {
        return multiSlaveShards;
    }

    public void setMultiSlaveShards(Map<String, List<JedisShardInfo>> multiSlaveShards) {
        this.multiSlaveShards = multiSlaveShards;
    }
}
