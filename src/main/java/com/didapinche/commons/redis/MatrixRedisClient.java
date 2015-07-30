package com.didapinche.commons.redis;

import com.didapinche.commons.redis.exceptions.MultiKeyRedisClientException;
import com.didapinche.commons.redis.exceptions.RedisClientException;
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
public final class MatrixRedisClient extends AbstractRedisClient{

    private static Logger logger = LoggerFactory.getLogger(MatrixRedisClient.class);



    private RedisPool<ShardedJedis> redisPool;

    @Override
    protected <T> T execute(CallBack<T> callBack, boolean readonly,int retryTimes) {
        retryTimes ++;

        if(retryTimes > 3) {

            logger.error("have retried 3 times for redis command");
            throw new RedisClientException("have retried 3 times for redis command");
        }

        if (readonly && autoReadFromSlave && redisPool.hasSlave()) {
            ShardedJedis shardedJedis = null;

            try {
                shardedJedis = redisPool.getSlaveResource();
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
                    redisPool.returnSlaveResourceObject(shardedJedis);
                }
            }

        } else {

            ShardedJedis shardedJedis = null;

            try {
                shardedJedis = redisPool.getMasterResource();
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
                    redisPool.returnMasterResourceObject(shardedJedis);
                }
            }

        }
    }

    @Override
    protected <T> T execute(MultiKeyCallBack<T> callBack, boolean readonly, int retryTimes) {
        throw new MultiKeyRedisClientException();
    }


    public void setRedisPool(RedisPool<ShardedJedis> redisPool) {
        this.redisPool = redisPool;
    }
}
