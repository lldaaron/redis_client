package com.didapinche.commons.redis;

import com.didapinche.commons.redis.sentinel.SentinelsManager;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author 罗立东 rod
 * @time 15/7/30
 */
@ContextConfiguration(locations = {"classpath*:spring_MasterSlave.xml"})
public class MasterSlaveRedisTest extends RedisTestBase {

    @Autowired
    private MasterSlaveRedisClient client;

    @Autowired
    private MasterSlaveRedisClient clientWithSentinel;


    /**
     * 1master 2slave
     */
    @Test
    public void testBasicReadWrite() throws InterruptedException {
        String key = "key";
        String value = "value";
        client.set(key, value);
        Assert.assertEquals(value, client.get(key));

        client.pexpire(key, 100);
        Assert.assertTrue(client.exists(key));

        //主从复制有延时
        Thread.sleep(200);

        Assert.assertFalse(client.exists(key));

    }

    /**
     * HA主动发现 自动配置初始化
     */
    @Test
    public void testHAInitAutoConf() throws InterruptedException {
        //初始化sentinelsManager
        SentinelsManager sentinelsManager = (SentinelsManager) applicationContext.getBean("sentinelsManager");

        Jedis sentinelJedis = (Jedis) applicationContext.getBean("sentinelJedis");

        //wait to conf
        Thread.sleep(1000);

        MasterSlaveRedisPool masterSlaveRedisPool = clientWithSentinel.getPool();

        //sentinelsManager需要管理的mastername
        String masterName = sentinelsManager.getSentinelInfo().getMasterNames().get(0);

        //获取master info
        List<String> masterInfo = sentinelJedis.sentinelGetMasterAddrByName(masterName);

        String masterHost = masterInfo.get(0);
        int masterPort = Integer.parseInt(masterInfo.get(1));

        //比对master
        HostAndPort masterHap = masterSlaveRedisPool.getMasterHap();
        Assert.assertTrue(masterHap.equals(new HostAndPort(masterHost, masterPort)));

        //获取slave info
        List<Map<String, String>> slavesInfo = sentinelJedis.sentinelSlaves(masterName);

        List<HostAndPort> slavesHap = masterSlaveRedisPool.getSlaveHaps();

        //验证slave数量
        Assert.assertEquals(slavesHap.size(), slavesInfo.size());

        for (Map<String, String> slaveInfo : slavesInfo) {
            String slaveHost = slaveInfo.get("ip");
            int slavePort = Integer.parseInt(slaveInfo.get("port"));
            HostAndPort slaveHap = new HostAndPort(slaveHost, slavePort);
            slavesHap.remove(slaveHap);
        }

        Assert.assertEquals(0, slavesHap.size());

    }


    /**
     * 在读写的情况下 slave下线
     * 测试时，请确保master和slave的上线状态。
     */
    @Test
    public void testHAsdownSlaveWhenReadAndWrite() throws InterruptedException {
        //初始化sentinelsManager
        applicationContext.getBean("sentinelsManager");
        final Jedis slaveJedis80 = (Jedis) applicationContext.getBean("slaveJedis80");
        final Jedis slaveJedis81 = (Jedis) applicationContext.getBean("slaveJedis81");

        //wait to conf
        Thread.sleep(1000);

        //begin slave down after 2s
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1 * 1000L);
                    slaveJedis80.shutdown();
                    logger.info("shutdown the slave 80");
                    Thread.sleep(1 * 1000L);
                    slaveJedis81.shutdown();
                    logger.info("shutdown the slave 81");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        //begin read and write

        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

        for (int i = 0; i < 10; i++) {
            int span = 8000;
            threadPoolExecutor.execute(new ConcurrentReadAndWriteTask(i,span));
        }
        threadPoolExecutor.shutdown();
        try {
            boolean loop = true;
            do {
                loop = !threadPoolExecutor.awaitTermination(2, TimeUnit.SECONDS);
            } while(loop);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class ConcurrentReadAndWriteTask implements Runnable {

        int index;
        int span;

        public ConcurrentReadAndWriteTask(int index, int span) {
            this.index = index;
            this.span = span;
        }

        @Override
        public void run() {
            int begin = index * span;
            int end = (index + 1) * span;
            while (begin < end) {
                if (Thread.currentThread().isInterrupted())
                    break;
                clientWithSentinel.set("key" + begin, "value" + begin);
//                System.out.println("set key:" + begin + ",and value:" + begin);
                clientWithSentinel.get("key" + begin);
//                System.out.println("get key:" + begin + ",and value:" + begin);
                begin ++ ;
            }
        }
    }

    /**
     * 在读写的情况下 slave上线
     * 测试时，请保证master的上线状态 和 slave的下线状态
     */
    @Test
    public void testHAnsdownSlaveWhenReadAndWrite() throws InterruptedException {

        //初始化sentinelsManager
        applicationContext.getBean("sentinelsManager");

        //wait to conf
        Thread.sleep(1000);

        //begin read and write

        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

        for (int i = 0; i < 10; i++) {
            int span = 200000;
            threadPoolExecutor.execute(new ConcurrentReadAndWriteTask(i,span));
        }


        MasterSlaveRedisPool pool = clientWithSentinel.getPool();

        Assert.assertFalse(pool.hasSlave());

        System.out.println("请上线一台slave");
        Thread.sleep(15000);

        Assert.assertTrue(pool.hasSlave());
        Assert.assertEquals(pool.getSlaveHaps().size(),1);

        System.out.println("请继续上线一台slave");
        Thread.sleep(15000);

        Assert.assertEquals(pool.getSlaveHaps().size(),2);

        threadPoolExecutor.shutdownNow();

        try {
            boolean loop = true;
            do {
                loop = !threadPoolExecutor.awaitTermination(2, TimeUnit.SECONDS);
            } while(loop);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
