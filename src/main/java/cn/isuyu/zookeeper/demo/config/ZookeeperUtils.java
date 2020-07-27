package cn.isuyu.zookeeper.demo.config;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

/**
 * @Author NieZhiLiang
 * @Email nzlsgg@163.com
 * @GitHub https://github.com/niezhiliang
 * @Date 2020/7/27 下午1:57
 */
public class ZookeeperUtils {

    private static ZooKeeper zooKeeper;

    private static final String ADDRESS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183/locks";

    private static CountDownLatch countDownLatch = new CountDownLatch(1);


    public static ZooKeeper getZk() {
        try {
            zooKeeper = new ZooKeeper(ADDRESS,3000,new DefaultWatch(countDownLatch));
            //阻塞等待zk连接完成
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return zooKeeper;
    }

}

class DefaultWatch implements Watcher {

    private CountDownLatch countDownLatch;

    public DefaultWatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public void process(WatchedEvent event) {
        switch (event.getState()) {
            case Unknown:
                break;
            case Disconnected:
                break;
            case NoSyncConnected:
                break;
            case SyncConnected:
                System.out.println("zk连接成功。。。。");
                //拿到连接后放行
                countDownLatch.countDown();
                break;
            case AuthFailed:
                break;
            case ConnectedReadOnly:
                break;
            case SaslAuthenticated:
                break;
            case Expired:
                break;
            case Closed:
                System.out.println("zk关闭。。。。");
                break;
        }
    }
}
