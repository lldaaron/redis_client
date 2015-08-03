package com.didapinche.commons.redis.sentinel;

import com.didapinche.commons.redis.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 订阅sentinel频道的消息
 *
 * @author 罗立东 rod
 * @time 15/7/29
 *
 *
 */
public class MasterListener implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(MasterListener.class);

    /**
     * 切换master标签
     */
    public final static String SWITCH_MASTER = "+switch-master";
    /**
     * 主观下线标签
     */
    public final static String S_DOWN = "+sdown";

    /**
     * 主观上线标签（不再是主观下线的状态）
     */
    public final static String N_S_DOWN = "-sdown";

    /**
     * 监控的主机名称集合
     */
    private List<String> masterNames;

    private String sentinelHost;
    private int sentinePort;

    /**
     * 等待重试时间
     */
    private long subscribeRetryWaitTimeMillis = 5000;

    private AtomicBoolean isRunning = new AtomicBoolean(false);

    private JedisPubSub jedisPubSub = new JedisPubSubCallBack();

    /**
     * HA动作执行者
     */
    private SentinelActor sentinelActor;

    public MasterListener(List<String> masterNames, String sentinelHost, int sentinePort, SentinelActor sentinelActor) {
        this.masterNames = masterNames;
        this.sentinelHost = sentinelHost;
        this.sentinePort = sentinePort;
        this.sentinelActor = sentinelActor;
    }

    public MasterListener(List<String> masterNames, String sentinelHost, int sentinePort, long subscribeRetryWaitTimeMillis, SentinelActor sentinelActor) {
        this(masterNames, sentinelHost, sentinePort, sentinelActor);
        this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
    }

    @Override
    public void run() {

        isRunning.set(true);

        while (isRunning.get()) {

            try (Jedis sentinelJedis = new Jedis(sentinelHost, sentinePort)) {

                sentinelJedis.subscribe(jedisPubSub, SWITCH_MASTER, S_DOWN, N_S_DOWN);

            } catch (JedisConnectionException e) {

                if (isRunning.get()) {
                    logger.warn("Lost connection to Sentinel at " + sentinelHost + ":" + sentinePort
                            + ". Sleeping " + subscribeRetryWaitTimeMillis + "ms and retrying.");
                    try {
                        Thread.sleep(subscribeRetryWaitTimeMillis);
                    } catch (InterruptedException e1) {
                        logger.error("InterruptedException", e);
                    }
                } else {
                    logger.info("Unsubscribing from Sentinel at " + sentinelHost + ":" + sentinePort);
                }
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }
    }

    public void shutdown() {
        try {
            logger.info("Shutting down listener on " + sentinelHost + ":" + sentinePort);
            isRunning.set(false);
            jedisPubSub.unsubscribe();
        } catch (Exception e) {
            logger.error("Caught exception while shutting down: ", e);
        }
    }



    /**
     * 监听事件回调
     */
    private class JedisPubSubCallBack extends JedisPubSub {

        @Override
        public void onMessage(String channel, String message) {

            logger.info("Sentinel " + sentinelHost + ":" + sentinePort + " published: " + message + ".");

            String[] messageArray = message.split(" ");

            if (channel.equals(S_DOWN)) {
                if (!masterNames.contains(messageArray[5])) {
                    logger.info("Ignoring message on " + S_DOWN + " for master name " + messageArray[5] + ".");
                } else if (messageArray[0].equals("master")) {
                    return;//master下线，忽略 ,等待+switch-master
                } else { //从机下线
                    HostAndPort downSlave = Utils.toHostAndPort(Arrays.asList(messageArray[2], messageArray[3]));
                    sentinelActor.sdownSlave(messageArray[5], downSlave);
                }
            } else if (channel.equals(N_S_DOWN)) {
                if (!masterNames.contains(messageArray[5])) {
                    logger.info("Ignoring message on " + N_S_DOWN + " for master name " + messageArray[5] + ".");
                } else if (messageArray[0].equals("master")) {
                    return;//master上线，忽略 ,等待+switch-master （应该不会发生。。。）
                } else { //从机上线
                    HostAndPort nDownSlave = Utils.toHostAndPort(Arrays.asList(messageArray[2], messageArray[3]));
                    sentinelActor.nsdownSlave(messageArray[5], nDownSlave);
                }

            } else if (channel.equals(SWITCH_MASTER) && messageArray.length > 3) {
                if (masterNames.contains(messageArray[0])) {
                    HostAndPort newMaster = Utils.toHostAndPort(Arrays.asList(messageArray[3], messageArray[4]));
                    sentinelActor.switchMaster(messageArray[0], newMaster);
                } else {
                    logger.info("Ignoring message on +switch-master for master name " + messageArray[0] + ".");
                }
            } else {
                logger.warn("Invalid message received on Sentinel " + sentinelHost + ":" + sentinePort
                        + " on channel +switch-master: " + message);
            }
        }
    }
}
