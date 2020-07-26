package cn.isuyu.zookeeper.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //为了让zk连接成功以后，再执行下面的操作(zk连接是异步的)
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final ZooKeeper zooKeeper = new ZooKeeper("192.168.0.107:2181,192.168.0.112:2181,192.168.0.109:2181,192.168.0.110:2181",
                10000, new Watcher() {
            public void process(WatchedEvent event) {
                Event.KeeperState state = event.getState();
                Event.EventType type = event.getType();
                System.out.println("new zk watch" + event.getPath());
                switch (state) {
                    case Unknown:
                        break;
                    case Disconnected:
                        break;
                    case NoSyncConnected:
                        break;
                    case SyncConnected:
                        System.out.println("连接成功");
                        //执行下面的操作
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
                        System.out.println("zk 连接关闭");
                        break;
                }
                switch (type) {
                    case None:
                        break;
                    case NodeCreated:
                        System.out.println("created");
                        break;
                    case NodeDeleted:
                        break;
                    case NodeDataChanged:
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
        });
        countDownLatch.await();
        ZooKeeper.States state = zooKeeper.getState();
        switch (state) {
            case CONNECTING:
                System.out.println("ing");
                break;
            case ASSOCIATING:
                break;
            case CONNECTED:
                System.out.println("connected");
                break;
            case CONNECTEDREADONLY:
                break;
            case CLOSED:
                break;
            case AUTH_FAILED:
                break;
            case NOT_CONNECTED:
                break;
        }

        /**
         * 创建临时节点
         * path
         * 内容
         * 访问类型
         * 节点类型
         */
        String xxoo = zooKeeper.create("/xxoo", "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(xxoo);

        final Stat stat = new Stat();
        /**
         * 同步watch（watch是一次性的）
         */
        byte[] data = zooKeeper.getData("/xxoo", new Watcher() {
            public void process(WatchedEvent event) {
                switch (event.getType()) {
                    case None:
                        break;
                    case NodeCreated:
                        break;
                    case NodeDeleted:
                        System.out.println("数据被删除触发回调。。。。");
                        break;
                    case NodeDataChanged:
                        System.out.println("数据改变触发回调。。。");
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
                System.out.println("watch from data :" + event.toString());
                /**
                 * 为了一直监听节点变化，加上这行代码
                 */
                try {
                    zooKeeper.getData("/xxoo",this,stat);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, stat);
        System.out.println(new String(data));

        System.out.println(stat);


        Stat stat1 = zooKeeper.setData("/xxoo", "hello world".getBytes(), 0);

        System.out.println(stat1);

        //zooKeeper.delete("/xxoo",1);
        //stat1 = zooKeeper.setData("/xxoo", "hello world".getBytes(), 1);

        /**
         * 异步获取节点数据
         */
        System.out.println("-----------------async begin-----------------");
        zooKeeper.getData("/xxoo", false, new AsyncCallback.DataCallback() {
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                System.out.println("-----------------async call back-----------------");
                System.out.println(ctx.toString());
                System.out.println(new String(data));

            }
        },"abc");
        System.out.println("-----------------async end-----------------");

        //zooKeeper.close();

        TimeUnit.SECONDS.sleep(500);
    }
}
