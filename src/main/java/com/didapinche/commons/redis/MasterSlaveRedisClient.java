package com.didapinche.commons.redis;

import com.didapinche.commons.redis.exceptions.RedisClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;


/**
 * MuiltRedisClient.java
 * Project: redis client
 * <p/>
 * File Created at 2015-7-28 by fengbin
 * <p/>
 * Copyright 2015 didapinche.com
 * <p/>
 * modified at 15/7/30 by 罗立东 rod
 */
public final class MasterSlaveRedisClient extends AbstractRedisClient {
    private static final Logger logger = LoggerFactory.getLogger(MasterSlaveRedisClient.class);


    private MasterSlaveRedisPool pool;

    protected <T> T execute(CallBack<T> callBack, boolean readonly, int retryTimes) {

        retryTimes++;

        if (retryTimes > 3) {

            logger.error("have retried 3 times for redis command");
            throw new RedisClientException("have retried 3 times for redis command");
        }

        if (readonly && autoReadFromSlave && pool.hasSlave()) {
            Jedis jedis = null;

            try {
                jedis = pool.getSlaveResource();
                return callBack.execute(jedis);
            } catch (JedisConnectionException e) {
                logger.error(e.getMessage());
                if (jedis != null) {
                    //从机实际下线
                    String host = jedis.getClient().getHost();
                    int port = jedis.getClient().getPort();
                    pool.sdownSlave(null, new HostAndPort(host, port));
                }
                return execute(callBack, readonly, retryTimes);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return execute(callBack, readonly, retryTimes);
            } finally {
                if (jedis != null) {
                    try {
                        pool.returnSlaveResourceObject(jedis);
                    } catch (Exception e) {
                        logger.error("occur exception while close the jedis");
                    }
                }
            }

        } else {

            Jedis jedis = null;

            try {
                jedis = pool.getMasterResource();
                T result = callBack.execute(jedis);
                return result;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return execute(callBack, readonly, retryTimes);
            } finally {
                if (jedis != null) {
                    pool.returnMasterResourceObject(jedis);
                }
            }
        }
    }

    @Override
    protected <T> T execute(MultiKeyCallBack<T> callBack, boolean readonly, int retryTimes) {

        retryTimes++;

        if (retryTimes > 3) {

            logger.error("have retried 3 times for redis command");
            throw new RedisClientException("have retried 3 times for redis command");
        }

        if (readonly && autoReadFromSlave && pool.hasSlave()) {
            Jedis jedis = null;

            try {
                jedis = pool.getSlaveResource();
                return callBack.execute(jedis);
            } catch (JedisConnectionException e) {
                logger.error(e.getMessage());
                if (jedis != null) {
                    //从机实际下线
                    String host = jedis.getClient().getHost();
                    int port = jedis.getClient().getPort();
                    pool.sdownSlave(null, new HostAndPort(host, port));
                }
                return execute(callBack, readonly, retryTimes);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return execute(callBack, readonly, retryTimes);
            } finally {
                if (jedis != null) {
                    try {
                        pool.returnSlaveResourceObject(jedis);
                    } catch (Exception e) {
                        logger.error("occur exception while close the jedis");
                    }
                }
            }

        } else {

            Jedis jedis = null;

            try {
                jedis = pool.getMasterResource();
                T result = callBack.execute(jedis);
                return result;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return execute(callBack, readonly, retryTimes);
            } finally {
                if (jedis != null) {
                    pool.returnMasterResourceObject(jedis);
                }
            }
        }
    }


    public void setAutoReadFromSlave(boolean autoReadFromSlave) {
        this.autoReadFromSlave = autoReadFromSlave;
    }

    public void setPool(MasterSlaveRedisPool pool) {
        this.pool = pool;
    }

    public MasterSlaveRedisPool getPool() {
        return pool;
    }
}
