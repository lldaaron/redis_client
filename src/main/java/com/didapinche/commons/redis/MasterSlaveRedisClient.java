package com.didapinche.commons.redis;

import com.didapinche.commons.redis.exceptions.DiDaRedisClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;


/**
 * MasterSlaveRedisClient.java
 * Project: redis client
 *
 * File Created at 2015-7-28 by fengbin
 *
 * Copyright 2015 didapinche.com
 */
public final class MasterSlaveRedisClient extends AbstractRedisClient{
    private static final Logger logger = LoggerFactory.getLogger(MasterSlaveRedisClient.class);


    private MasterSlaveRedisPool pool;


    protected  <T> T execute(AbstractRedisClient.CallBack<T> callBack) {
        return execute(callBack,false);
    }

    protected <T> T execute(AbstractRedisClient.CallBack<T> callBack,boolean readonly){
        return execute(callBack,readonly,0);
    }
    protected <T> T execute(AbstractRedisClient.CallBack<T> callBack,boolean readonly,int retryTimes){

        retryTimes ++;

        if(retryTimes > 3 ) {

            logger.error("have retried 3 times for redis command");
            throw new DiDaRedisClientException("have retried 3 times for redis command");
        }

        if (pool.getJedisPool() != null ) {
            Jedis jedis = null;

            try {
                jedis = pool.getResource();
                return callBack.execute(jedis);
            }catch (Exception e){
                logger.error(e.getMessage(),e);
                return execute(callBack,readonly,retryTimes);
            }finally {
                if(jedis != null) {
                    pool.returnResourceObject(jedis);
                }
            }

        } else if (readonly && autoReadFromSlave && pool.getSlaveShardedJedisPool() != null) {
            ShardedJedis shardedJedis = null;

            try {
                shardedJedis = pool.getSlaveResource();
                return callBack.execute(shardedJedis);
            }catch (Exception e){
                logger.error(e.getMessage(),e);
                return execute(callBack,readonly,retryTimes);
            }finally {
                if(shardedJedis != null) {
                    pool.returnSlaveResourceObject(shardedJedis);
                }
            }

        } else if (pool.getMasterShardedJedisPool() != null) {

            ShardedJedis shardedJedis = null;

            try {
                shardedJedis = pool.getMasterResource();
                T result = callBack.execute(shardedJedis);
                return result;
            }catch (Exception e){
                logger.error(e.getMessage(),e);
                return execute(callBack,readonly,retryTimes);
            }finally {
                if(shardedJedis != null) {
                    pool.returnMasterResourceObject(shardedJedis);
                }
            }

        } else {
            throw new DiDaRedisClientException("jedisPool or masterShards master configured one.");
        }
    }


    public void setAutoReadFromSlave(boolean autoReadFromSlave) {
        this.autoReadFromSlave = autoReadFromSlave;
    }

    public void setPool(MasterSlaveRedisPool pool) {
        this.pool = pool;
    }
}
