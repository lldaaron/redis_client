package com.didapinche.commons.redis;

import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;

import java.util.*;

/**
 * Created by fengbin on 15/7/29.
 */
public class RedisSentinelTest extends RedisTestBase {

    @Test
    public void testInitPool(){

    }


    @Test
    public void switchMaster(){

    }


    @Test
    public void testSdown(){

    }


    @Test
    public void test$Sdown(){

    }



    @Test
    public void testGetMasterResource(){

        ShardedJedis shardedJedis = redisSentinelClient.getRedisSentinelPool().getMasterResource();
        Collection<JedisShardInfo> shardInfoList = shardedJedis.getAllShardInfo();


        for(JedisShardInfo shardInfo:shardInfoList){
            Assert.assertTrue(Constants.DEFAULT_MASTER1_MASTER.equals(shardInfo.getHost() + ":" + shardInfo.getPort())
                    || Constants.DEFAULT_MASTER2_MASTER.equals(shardInfo.getHost() + ":" + shardInfo.getPort()));
        }
    }



    @Test
    public void testGetSlaveResource(){
        ShardedJedis shardedJedis = redisSentinelClient.getRedisSentinelPool().getSlaveResource();
        Collection<JedisShardInfo> shardInfoList = shardedJedis.getAllShardInfo();

        for(JedisShardInfo shardInfo:shardInfoList){
            Assert.assertTrue(Arrays.asList(Constants.DEFAULT_MASTER1_SLAVES).contains(shardInfo.getHost() + ":" + shardInfo.getPort())
                    || Arrays.asList(Constants.DEFAULT_MASTER2_SLAVES).contains(shardInfo.getHost() + ":" + shardInfo.getPort()));
        }
    }


    @Test
    public void testHashDiff() {
        RedisSentinelPool pool = redisSentinelClient.getRedisSentinelPool();
        ShardedJedis masterShardedJedis = pool.getMasterResource();
        ShardedJedis slaveShardedJedis = pool.getSlaveResource();
        Map<String, JedisShardInfo> masterShards =  pool.getMasterShards();
        //Map<String, JedisShardInfo> mu =  pool.getMultiSlaveShards();

        Map<String, String> masterSearchMap =  new HashMap<>();

        for(String masterName:masterShards.keySet()){
            JedisShardInfo shardInfo = masterShards.get(masterName);
            masterSearchMap.put(shardInfo.getHost()+":"+shardInfo.getHost(),masterName);
        }

        for(String masterName:masterShards.keySet()){
            JedisShardInfo shardInfo = masterShards.get(masterName);
            masterSearchMap.put(shardInfo.getHost()+":"+shardInfo.getHost(),masterName);
        }




        for (int i =0 ;i < 10 ;i++){
            String randomKey = String.valueOf(new Random().nextInt(100000));

            JedisShardInfo masterShardInfo = masterShardedJedis.getShardInfo(randomKey);
            JedisShardInfo slaveShardInfo = slaveShardedJedis.getShardInfo(randomKey);

            String masterMasterName = masterSearchMap.get(masterShardInfo .getHost() + ":" + masterShardInfo.getPort());
            String slaveMasterName = masterSearchMap.get(slaveShardInfo .getHost() + ":" + slaveShardInfo.getPort());

            Assert.assertEquals(masterMasterName, slaveMasterName);

        }


    }

}
