package cn.isuyu.zookeeper.demo.config;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

/**
 * @Author NieZhiLiang
 * @Email nzlsgg@163.com
 * @GitHub https://github.com/niezhiliang
 * @Date 2020/7/27 下午2:07
 */
public class MyPropertiesWatch implements Watcher, AsyncCallback.StatCallback, AsyncCallback.DataCallback {

    /**
     * 配置接收变量
     */
    private MyProperties myProperties;

    private ZooKeeper zooKeeper;

    /**
     * 统一配置中心节点名称
     */
    private String path;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public void setPath(String path) {
        this.path = path;
    }

    public void setMyProperties(MyProperties myProperties) {
        this.myProperties = myProperties;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    /**
     * 如果节点没创建进入等待状态
     */
    public void waitData() {
        try {
            zooKeeper.exists(path,this,this,"conf");
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 节点变化监控
     * @param event
     */
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                break;
            case NodeCreated:
                System.out.println("统一配置节点创建。。。。");
                zooKeeper.getData(path,this,this,"conf");
                break;
            case NodeDeleted:
                System.out.println("统一配置节点删除。。。。");
                zooKeeper.exists(path,this,this,"conf");
                break;
            case NodeDataChanged:
                System.out.println("统一配置节点属性值变更。。。。");
                zooKeeper.getData(path,this,this,"conf");
                break;
            case NodeChildrenChanged:
                break;
            case DataWatchRemoved:
                break;
            case ChildWatchRemoved:
                break;
            case PersistentWatchRemoved:
                break;
        }
    }

    /**
     * exists回调
     * @param rc
     * @param path
     * @param ctx
     * @param stat
     */
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        if (stat != null) {
            zooKeeper.getData(path,this,this,"conf");
        }
    }

    /**
     * getData回调
     * @param rc
     * @param path
     * @param ctx
     * @param data
     * @param stat
     */
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        myProperties.setData(new String(data));
        countDownLatch.countDown();
    }
}
