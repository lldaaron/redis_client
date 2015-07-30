package com.didapinche.commons.redis;

import com.didapinche.commons.redis.exceptions.DiDaRedisClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.ShardedJedis;

/**
 * Project: redis client
 *
 * File Created at 2015-7-28 by fengbin
 *
 * Copyright 2015 didapinche.com
 */
public final class MultiRedisClient extends AbstractRedisClient{

    private static Logger logger = LoggerFactory.getLogger(MultiRedisClient.class);



    private MultiRedisPool redisSentinelPool;

    @Override
    protected <T> T execute(CallBack<T> callBack) {
        return execute(callBack,false);
    }

    @Override
    protected <T> T execute(CallBack<T> callBack, boolean readonly) {
        return execute(callBack,false,0);

    }

    @Override
    protected <T> T execute(CallBack<T> callBack, boolean readonly,int retryTimes) {
        retryTimes ++;

        if(retryTimes > 3) {

            logger.error("have retried 3 times for redis command");
            throw new DiDaRedisClientException("have retried 3 times for redis command");
        }

        if (readonly && autoReadFromSlave && redisSentinelPool.hasSlave()) {
            ShardedJedis shardedJedis = null;

            try {
                shardedJedis = redisSentinelPool.getSlaveResource();
                T result = callBack.execute(shardedJedis);

                return result;
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
                if (readonly){
                    return execute(callBack, readonly, retryTimes);
                }
                throw e;
            }finally {
                if(shardedJedis != null) {
                    redisSentinelPool.returnSlaveResourceObject(shardedJedis);
                }
            }

        } else {

            ShardedJedis shardedJedis = null;

            try {
                shardedJedis = redisSentinelPool.getMasterResource();
                T result = callBack.execute(shardedJedis);

                return result;
            }catch (Exception e) {
                logger.warn(e.getMessage(),e);
                if (readonly){
                    return execute(callBack, readonly, retryTimes);
                }
                throw e;
            }finally {
                if(shardedJedis != null) {
                    redisSentinelPool.returnMasterResourceObject(shardedJedis);
                }
            }

        }
    }
    public MultiRedisPool getRedisSentinelPool() {
        return redisSentinelPool;
    }

    public void setRedisSentinelPool(MultiRedisPool redisSentinelPool) {
        this.redisSentinelPool = redisSentinelPool;
    }
}
