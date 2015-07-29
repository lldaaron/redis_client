package com.didapinche.commons.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

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
 *                <bean class="com.didapinche.commons.redis.SentinelInfo">
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
 *                <bean class="com.didapinche.commons.redis.SentinelInfo">
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
public class RedisSentinelPool implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(SentinelRedisClient.class);
    //纪录线程池
    private ThreadLocal<ShardedJedisPool> slavePoolTh = new ThreadLocal();


    private JedisPoolConfig jedisPoolConfig;
    private Map<String,SentinelInfo> sentinelShards;

    //主从配置信息
    private Map<String,JedisShardInfo> masterShards = new HashMap<>();
    private Map<String,List<JedisShardInfo>> multiSlaveShards = new HashMap();

    //直接使用ShardedJedisPool做hash
    private ShardedJedisPool masterShardedJedisPool;
    private List<ShardedJedisPool> slaveShardedJedisPools = new ArrayList<>();

    //监听redisSentinel
    private Set<MasterListener> masterListeners = new HashSet<MasterListener>();


    @Override
    public void afterPropertiesSet() throws Exception {

        if(sentinelShards != null) {

            initSentinelShards(sentinelShards);
        } else {
            initPool();
        }
    }


    /**
     * 是否存在slave
     * @return
     */
    public boolean hasSlave(){
        return (slaveShardedJedisPools != null && slaveShardedJedisPools.size() >0);
    }

    public ShardedJedis getMasterResource(){
        return masterShardedJedisPool.getResource();
    }
    public void returnMasterResourceObject(ShardedJedis jedis) {
        masterShardedJedisPool.returnResourceObject(jedis);
    }


    public ShardedJedis getSlaveResource(){

        int index = new Random().nextInt(slaveShardedJedisPools.size());
        ShardedJedisPool pool = slaveShardedJedisPools.get(index);
        slavePoolTh.set(pool);

        return pool.getResource();
    }



    public void returnSlaveResourceObject(ShardedJedis jedis) {
        slavePoolTh.get().returnResourceObject(jedis);
    }




    /**
     * 初始化一组sentinel监听服务，之后初始化master连接池和slave连接池
     * @param masterName
     * @param sentinelInfo
     * @return
     */
    private HostAndPort initOneSentinels(final String masterName,SentinelInfo sentinelInfo) {

        Set<String> sentinels = sentinelInfo.getSentinels();

        HostAndPort master = null;
        boolean sentinelAvailable = false;

        logger.info("Trying to find master from available Sentinels...");

        for (String sentinel : sentinels) {
            final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));

            logger.info("Connecting to Sentinel " + hap);

            Jedis jedis = null;
            try {
                jedis = new Jedis(hap.getHost(), hap.getPort());

                List<String> masterAddr = jedis.sentinelGetMasterAddrByName(masterName);

                // connected to sentinel...
                sentinelAvailable = true;

                if (masterAddr == null || masterAddr.size() != 2) {
                    logger.warn("Can not get master addr, master name: " + masterName + ". Sentinel: " + hap
                            + ".");
                    continue;
                }

                master = toHostAndPort(masterAddr);

                List<Map<String,String>>slaveInfo = jedis.sentinelSlaves(masterName);
                buildShardInfos(masterName, master, slaveInfo);


                logger.info("Found Redis master at " + master);
                break;
            } catch (JedisConnectionException e) {
                logger.warn("Cannot connect to sentinel running @ " + hap + ". Trying next one.");
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

        if (master == null) {
            if (sentinelAvailable) {
                // can connect to sentinel, but master name seems to not
                // monitored
                throw new JedisException("Can connect to sentinel, but " + masterName
                        + " seems to be not monitored...");
            } else {
                throw new JedisConnectionException("All sentinels down, cannot determine where is "
                        + masterName + " master is running...");
            }
        }

        logger.info("Redis master running at " + master + ", starting Sentinel listeners...");

        for (String sentinel : sentinels) {
            final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
            MasterListener masterListener = new MasterListener(masterName, hap.getHost(), hap.getPort(),this);
            masterListeners.add(masterListener);
            masterListener.start();
        }

        return master;
    }


    private void initSentinelShards(Map<String,SentinelInfo> sentinelShards){

        for(String masterName:sentinelShards.keySet()){
            initOneSentinels(masterName,sentinelShards.get(masterName));
        }

        initPool();
    }


    private void initMasterPool(List<JedisShardInfo> masterShards) {
        if( masterShards != null && masterShards.size() > 0) {
            masterShardedJedisPool = new ShardedJedisPool(jedisPoolConfig,masterShards);
        }
    }

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



    public  void initPool(){

        initPool(new ArrayList<JedisShardInfo>(masterShards.values()),multiSlaveShards);
    }

    private void initPool(List<JedisShardInfo> masterShards,Map<String,List<JedisShardInfo>> multiSlaveShards){

        initMasterPool(masterShards);

        initSlavePool(multiSlaveShards);
    }



    public void switchMaster(String masterName, HostAndPort masterInfo){

        buildMasterShardInfos(masterName,masterInfo);
        initMasterPool(new ArrayList<JedisShardInfo>(masterShards.values()));
        //sdownSlave(masterName,masterInfo);

    }
    public void buildMasterShardInfos(String masterName, HostAndPort masterInfo){
        JedisShardInfo masterShardInfo = new JedisShardInfo(masterInfo.getHost(),masterInfo.getPort());

        masterShards.put(masterName,masterShardInfo);
    }

    public void buildShardInfos(String masterName, HostAndPort masterInfo, List<Map<String, String>> slaveInfos){
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
    public void $sdownSlave(String masterName,HostAndPort hostAndPort){
        List<JedisShardInfo> slaveInfos = multiSlaveShards.get(masterName);

        for(JedisShardInfo shardInfo:slaveInfos ){
            if(shardInfo.getHost().equals(hostAndPort.getHost()) && shardInfo.getPort() == hostAndPort.getPort()){
                return;
            }
        }

        slaveInfos.add(new JedisShardInfo(hostAndPort.getHost(),hostAndPort.getPort()));

        initSlavePool();
    }


    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));

        return new HostAndPort(host, port);
    }


    public Map<String, SentinelInfo> getSentinelShards() {
        return sentinelShards;
    }

    public void setSentinelShards(Map<String, SentinelInfo> sentinelShards) {
        this.sentinelShards = sentinelShards;
    }

    public void setJedisPoolConfig(JedisPoolConfig jedisPoolConfig) {
        this.jedisPoolConfig = jedisPoolConfig;
    }
}
