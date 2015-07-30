package com.didapinche.commons.redis.utils;

import redis.clients.jedis.HostAndPort;

import java.util.List;

/**
 * Created by fengbin on 15/7/30.
 */
public class Utils {


    public static HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));

        return new HostAndPort(host, port);
    }
}
