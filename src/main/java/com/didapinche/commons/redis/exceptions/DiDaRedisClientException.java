package com.didapinche.commons.redis.exceptions;

/**
 * DiDaRedisClientException.java
 * Project: redis client
 *
 * File Created at 2015-7-28 by fengbin
 *
 * Copyright 2015 didapinche.com
 */
public class DiDaRedisClientException extends RuntimeException {
    public DiDaRedisClientException(String msg){
         super(msg);
    }
}
