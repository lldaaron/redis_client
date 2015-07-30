package com.didapinche.commons.redis.exceptions;

/**
 * RedisClientException.java
 * Project: redis client
 *
 * File Created at 2015-7-28 by fengbin
 *
 * Copyright 2015 didapinche.com
 */
public class MultiKeyRedisClientException extends RuntimeException {
    public MultiKeyRedisClientException(){
         super("multi key commands not support in MatrixRedisClient");
    }
}
