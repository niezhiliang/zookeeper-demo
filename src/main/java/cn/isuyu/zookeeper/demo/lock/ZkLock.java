package cn.isuyu.zookeeper.demo.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Author NieZhiLiang
 * @Email nzlsgg@163.com
 * @GitHub https://github.com/niezhiliang
 * @Date 2020/7/27 下午3:33
 */
public class ZkLock implements Watcher,AsyncCallback.Create2Callback, AsyncCallback.Children2Callback {

    private String threadName;

    private ZooKeeper zooKeeper;

    private String lock;

    private String lockName;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public ZkLock(String threadName,ZooKeeper zooKeeper, String lock) {
        this.threadName = threadName;
        this.zooKeeper = zooKeeper;
        this.lock = lock;
    }

    /**
     * 加锁代码
     */
    public void tryLock() {
        try {
            //创建临时节点
            zooKeeper.create(lock,threadName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,this,"lock");
            //阻塞，等获取到锁以后继续往下执行
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 解锁
     */
    public void unLock() {
        try {
            zooKeeper.delete("/"+lockName,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建临时节点后回调方法
     * @param rc
     * @param path
     * @param ctx
     * @param name
     * @param stat
     */
    @Override
    public void processResult(int rc, String path, Object ctx, String name, Stat stat) {
        if (stat != null) {
            System.out.println(threadName + "创建成功子节点："+ name);
            lockName = name.substring(1);
            //创建成功以后，取根目录下所有的临时节点，触发回调
            zooKeeper.getChildren("/", false, this, ctx);
        }
    }

    /**
     * 节点删除后回调
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                break;
            case NodeCreated:
                break;
            case NodeDeleted:
                //重新获取所有临时节点，触发排序放行
                zooKeeper.getChildren("/",false,this,"");
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

    /**
     * 执行getChildren()回调的方法
     * @param rc
     * @param path
     * @param ctx
     * @param children
     * @param stat
     */
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        //先对临时节点排序，查看自己是不是集合中第一个
        Collections.sort(children);
        int i = children.indexOf(lockName);
        if (i < 1) {
            System.out.println(threadName+"拿到锁，开始干活。。。。");
            //打开门栓放行
            countDownLatch.countDown();
        } else {
            try {
                System.out.println(threadName + " watch:" + children.get(i - 1));
                //没拿到锁的watch自己的前一个节点
                zooKeeper.exists("/" + children.get(i - 1),this);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
