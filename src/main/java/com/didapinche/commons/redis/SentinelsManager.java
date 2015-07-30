package com.didapinche.commons.redis;

import com.didapinche.commons.redis.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.util.*;

/**
 * Created by fengbin on 15/7/30.
 */
public class SentinelsManager {
    private static final Logger logger = LoggerFactory.getLogger(SentinelRedisClient.class);

    //监听redisSentinel
    private Set<MasterListener> masterListeners = new HashSet<MasterListener>();

    /**
     * 初始化一组sentinel监听服务，之后初始化master连接池和slave连接池
     * @param masterName
     * @param sentinelInfo
     * @return
     */
    private HostAndPort initOneSentinels(final String masterName,SentinelInfo sentinelInfo) {

        Set<String> sentinels = sentinelInfo.getSentinels();

        HostAndPort master = null;
        boolean sentinelAvailable = false;

        logger.info("Trying to find master from available Sentinels...");

        for (String sentinel : sentinels) {
            final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));

            logger.info("Connecting to Sentinel " + hap);

            Jedis jedis = null;
            try {
                jedis = new Jedis(hap.getHost(), hap.getPort());

                List<String> masterAddr = jedis.sentinelGetMasterAddrByName(masterName);

                // connected to sentinel...
                sentinelAvailable = true;

                if (masterAddr == null || masterAddr.size() != 2) {
                    logger.warn("Can not get master addr, master name: " + masterName + ". Sentinel: " + hap
                            + ".");
                    continue;
                }

                master = toHostAndPort(masterAddr);

                List<Map<String,String>>slaveInfo = jedis.sentinelSlaves(masterName);
                buildShardInfos(masterName, master, slaveInfo);


                logger.info("Found Redis master at " + master);
                break;
            } catch (JedisConnectionException e) {
                logger.warn("Cannot connect to sentinel running @ " + hap + ". Trying next one.");
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

        if (master == null) {
            if (sentinelAvailable) {
                // can connect to sentinel, but master name seems to not
                // monitored
                throw new JedisException("Can connect to sentinel, but " + masterName
                        + " seems to be not monitored...");
            } else {
                throw new JedisConnectionException("All sentinels down, cannot determine where is "
                        + masterName + " master is running...");
            }
        }

        logger.info("Redis master running at " + master + ", starting Sentinel listeners...");

        for (String sentinel : sentinels) {
            final HostAndPort hap = Utils.toHostAndPort(Arrays.asList(sentinel.split(":")));
            MasterListener masterListener = new MasterListener(masterName, hap.getHost(), hap.getPort(),this);
            masterListeners.add(masterListener);
            masterListener.start();
        }

        return master;
    }


    private void initSentinelShards(Map<String,SentinelInfo> sentinelShards){

        for(String masterName:sentinelShards.keySet()){
            initOneSentinels(masterName,sentinelShards.get(masterName));
        }

        initPool();
    }
}
