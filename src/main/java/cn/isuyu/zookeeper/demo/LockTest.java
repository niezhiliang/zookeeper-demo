package cn.isuyu.zookeeper.demo;

import cn.isuyu.zookeeper.demo.config.ZookeeperUtils;
import cn.isuyu.zookeeper.demo.lock.ZkLock;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @Author NieZhiLiang
 * @Email nzlsgg@163.com
 * @GitHub https://github.com/niezhiliang
 * @Date 2020/7/27 下午3:33
 */
public class LockTest {

    ZooKeeper zooKeeper;

    static final String LOCK = "/myLock";

    @Before
    public void before() {
        zooKeeper = ZookeeperUtils.getZk();
    }

    @After
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Test
    public void testLock() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(10);
        for (int i = 0; i <10 ; i++) {
            new Thread(()->{
                ZkLock zkLock = new ZkLock(Thread.currentThread().getName(),zooKeeper,LOCK);
                zkLock.tryLock();
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                zkLock.unLock();
                countDownLatch.countDown();
            }).start();
        }
        countDownLatch.await();
        System.out.println("线程干活完毕。。。。。");

    }

}
