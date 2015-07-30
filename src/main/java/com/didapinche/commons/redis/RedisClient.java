package com.didapinche.commons.redis;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.MultiKeyCommands;

/**
 * Created by fengbin on 15/7/29.
 */
public interface RedisClient extends JedisCommands,MultiKeyCommands {
    public Double hincrByFloat(final String key, final String field, final double value);
}
