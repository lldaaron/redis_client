package com.didapinche.commons.redis;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.MultiKeyCommands;


/**
 * RedisClientException.java
 * Project: redis client
 *
 * File Created at 2015-7-30 by fengbin
 *
 * Copyright 2015 didapinche.com
 */
public interface RedisClient extends JedisCommands,MultiKeyCommands {
    Double hincrByFloat(final String key, final String field, final double value);
}
