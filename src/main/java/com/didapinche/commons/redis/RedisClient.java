package com.didapinche.commons.redis;

import redis.clients.jedis.JedisCommands;

/**
 * Created by fengbin on 15/7/29.
 */
public interface RedisClient extends JedisCommands {
    public Double hincrByFloat(final String key, final String field, final double value);
}
