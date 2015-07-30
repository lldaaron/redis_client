package com.didapinche.commons.redis.sentinel;

import redis.clients.jedis.HostAndPort;

/**
 * 定义了一些高可用的动作（failover等）
 *
 * @author 罗立东 rod
 * @time 15/7/29
 */
public interface SentinelActor {

    /**
     * 切换master （注意：需要相应的slave）
     * @param masterName
     * @param newMasterHostAndPort 新的master连接
     */
    public void switchMaster(String masterName, HostAndPort newMasterHostAndPort);

    /**
     * 从机下线
     * @param masterName
     * @param hostAndPort
     */
    public void sdownSlave(String masterName, HostAndPort hostAndPort);

    /**
     * 从机上线
     * @param masterName
     * @param hostAndPort
     */
    public void nsdownSlave(String masterName, HostAndPort hostAndPort);
}
