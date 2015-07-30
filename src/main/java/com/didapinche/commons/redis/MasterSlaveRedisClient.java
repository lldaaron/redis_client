package com.didapinche.commons.redis;

import com.didapinche.commons.redis.exceptions.RedisClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;


/**
 * MuiltRedisClient.java
 * Project: redis client
 *
 * File Created at 2015-7-28 by fengbin
 *
 * Copyright 2015 didapinche.com
 */
public final class MasterSlaveRedisClient extends AbstractRedisClient{
    private static final Logger logger = LoggerFactory.getLogger(MasterSlaveRedisClient.class);


    private MasterSlaveRedisPool pool;


    protected  <T> T execute(CallBack<T> callBack) {
        return execute(callBack,false);
    }

    protected <T> T execute(CallBack<T> callBack,boolean readonly){
        return execute(callBack,readonly,0);
    }
    protected <T> T execute(CallBack<T> callBack,boolean readonly,int retryTimes){

        retryTimes ++;

        if(retryTimes > 3 ) {

            logger.error("have retried 3 times for redis command");
            throw new RedisClientException("have retried 3 times for redis command");
        }

       if (readonly && autoReadFromSlave && pool.getMasterJedisPool()!= null) {
            Jedis jedis = null;

            try {
                jedis = pool.getSlaveResource();
                return callBack.execute(jedis);
            }catch (Exception e){
                logger.error(e.getMessage(),e);
                return execute(callBack,readonly,retryTimes);
            }finally {
                if(jedis != null) {
                    pool.returnSlaveResourceObject(jedis);
                }
            }

        } else if (pool.getSlaveJedisPool() != null) {

            Jedis jedis = null;

            try {
                jedis = pool.getMasterResource();
                T result = callBack.execute(jedis);
                return result;
            }catch (Exception e){
                logger.error(e.getMessage(),e);
                return execute(callBack,readonly,retryTimes);
            }finally {
                if(jedis != null) {
                    pool.returnMasterResourceObject(jedis);
                }
            }

        } else {
            throw new RedisClientException("jedisPool or masterShards master configured one.");
        }
    }

    @Override
    protected <T> T execute(MultiKeyCallBack<T> callBack) {
        return null;
    }

    @Override
    protected <T> T execute(MultiKeyCallBack<T> callBack, boolean readonly) {
        return null;
    }

    @Override
    protected <T> T execute(MultiKeyCallBack<T> callBack, boolean readonly, int retryTimes) {
        return null;
    }


    public void setAutoReadFromSlave(boolean autoReadFromSlave) {
        this.autoReadFromSlave = autoReadFromSlave;
    }

    public void setPool(MasterSlaveRedisPool pool) {
        this.pool = pool;
    }
}
