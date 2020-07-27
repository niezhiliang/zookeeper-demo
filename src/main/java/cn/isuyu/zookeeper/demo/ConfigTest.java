package cn.isuyu.zookeeper.demo;

import cn.isuyu.zookeeper.demo.config.MyProperties;
import cn.isuyu.zookeeper.demo.config.MyPropertiesWatch;
import cn.isuyu.zookeeper.demo.config.ZookeeperUtils;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @Author NieZhiLiang
 * @Email nzlsgg@163.com
 * @GitHub https://github.com/niezhiliang
 * @Date 2020/7/27 下午2:07
 */
public class ConfigTest {

    ZooKeeper zooKeeper;

    private static final String PATH = "/conf";

    @Before
    public void before() {
        zooKeeper = ZookeeperUtils.getZk();
    }

    @After
    public void close() throws Exception {
        zooKeeper.close();
    }

    @Test
    public void monitor() throws Exception {
        MyProperties myProperties = new MyProperties();

        MyPropertiesWatch watch = new MyPropertiesWatch();
        watch.setMyProperties(myProperties);
        watch.setZooKeeper(zooKeeper);
        watch.setPath(PATH);
        watch.waitData();
        while (true) {
            String data = myProperties.getData();
            if (data == null) {
                System.out.println("配置丢失。。。。");
            } else {
                System.out.println(myProperties.getData());
            }
            TimeUnit.SECONDS.sleep(2);
        }
    }

}
