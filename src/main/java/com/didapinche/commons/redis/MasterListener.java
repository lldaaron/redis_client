package com.didapinche.commons.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 * MasterListener.java 监听sentinel 的+sdown -sdown & +switch-master
 * Project: redis client
 *
 * File Created at 2015-7-28 by fengbin
 *
 * Copyright 2015 didapinche.com
 */
public class MasterListener extends Thread {
    private static Logger logger = LoggerFactory.getLogger(MasterListener.class);

    protected String masterName;
    protected String host;
    protected int port;
    protected long subscribeRetryWaitTimeMillis = 5000;
    protected Jedis j;
    protected AtomicBoolean running = new AtomicBoolean(false);


    private RedisSentinelPool pool;

    protected MasterListener() {
    }

    public MasterListener(String masterName, String host, int port,RedisSentinelPool pool) {
        this.masterName = masterName;
        this.host = host;
        this.port = port;
        this.pool = pool;
    }

    public MasterListener(String masterName, String host, int port,RedisSentinelPool pool,
                          long subscribeRetryWaitTimeMillis) {
        this(masterName, host, port,pool);
        this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;


    }

    public void run() {

        running.set(true);

        while (running.get()) {

            j = new Jedis(host, port);

            try {
                j.subscribe(new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {


                        logger.info("Sentinel " + host + ":" + port + " published: " + message + ".");

                        String[] switchMasterMsg = message.split(" ");

                        if(channel.equals("+sdown")) {
                            if(!masterName.equals(switchMasterMsg[5]) ){
                                logger.info("Ignoring message on +switch-master for master name "
                                        + switchMasterMsg[0] + ", our master name is " + masterName);

                                return;
                            }else if(switchMasterMsg[0].equals("master")) {

                                return;//忽略 “+sdown” ,等待+switch-master
                            } else {
                                HostAndPort slave = toHostAndPort(Arrays.asList(switchMasterMsg[2], switchMasterMsg[3]));
                                pool.sdownSlave(masterName,slave);
                                return;
                            }

                        }

                        if(channel.equals("-sdown")) {
                            if(!masterName.equals(switchMasterMsg[5]) ){
                                logger.info("Ignoring message on +switch-master for master name "
                                        + switchMasterMsg[0] + ", our master name is " + masterName);

                                return;
                            }else if(switchMasterMsg[0].equals("master")) {

                                return;//忽略 “+sdown” ,等待+switch-master
                            } else {
                                HostAndPort slave = toHostAndPort(Arrays.asList(switchMasterMsg[2], switchMasterMsg[3]));
                                pool.$sdownSlave(masterName, slave);
                                return;
                            }

                        }

                        // switch master
                        if (switchMasterMsg.length > 3) {

                            if (masterName.equals(switchMasterMsg[0])) {
                                HostAndPort master = toHostAndPort(Arrays.asList(switchMasterMsg[3], switchMasterMsg[4]));
                                pool.switchMaster(masterName, master);
                            } else {
                                logger.info("Ignoring message on +switch-master for master name "
                                        + switchMasterMsg[0] + ", our master name is " + masterName);
                            }
                        } else {
                            logger.warn("Invalid message received on Sentinel " + host + ":" + port
                                    + " on channel +switch-master: " + message);
                        }
                    }
                }, "+switch-master","+sdown","-sdown");


            }catch (JedisConnectionException e) {

                if (running.get()) {
                    logger.warn("Lost connection to Sentinel at " + host + ":" + port
                            + ". Sleeping 5000ms and retrying.");
                    try {
                        Thread.sleep(subscribeRetryWaitTimeMillis);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                } else {
                    logger.info("Unsubscribing from Sentinel at " + host + ":" + port);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }



    public void shutdown() {
        try {
            logger.info("Shutting down listener on " + host + ":" + port);
            running.set(false);
            // This isn't good, the Jedis object is not thread safe
            j.disconnect();
        } catch (Exception e) {
            logger.error("Caught exception while shutting down: ", e);
        }
    }



    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));

        return new HostAndPort(host, port);
    }

}
