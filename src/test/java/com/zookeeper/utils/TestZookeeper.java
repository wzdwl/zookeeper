package com.zookeeper.utils;

import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class TestZookeeper {
	
	public final static String CONNECTSTRING="192.168.112.128:2181";
	public final static int SESSION_TIMEOUT=50*1000;
	public final static String PATH="/atguigu";
	private static final Logger LOGGER = Logger.getLogger(TestZookeeper.class);
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		TestZookeeper zp = new TestZookeeper();
		ZooKeeper zooKeeper = zp.StartZookeeper();
		if(zooKeeper.exists(PATH, false)==null) {
			
			zp.createzNode(zooKeeper, PATH,"helloworld");
			String zNode = zp.getzNode(zooKeeper, PATH);
			
			LOGGER.info(zNode);
			zp.stopZookeeper(zooKeeper);
		}else {
			LOGGER.info("路径已存在");
		}
	}
	
	public ZooKeeper StartZookeeper() throws IOException {
		return new ZooKeeper(CONNECTSTRING, SESSION_TIMEOUT, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
			}
		});
	}
	
	public void stopZookeeper(ZooKeeper zooKeeper) throws InterruptedException {
		if(zooKeeper!=null) {
			zooKeeper.close();
		}
	}
	
	public void createzNode(ZooKeeper zooKeeper,String nodePath,String nodeValue) throws KeeperException, InterruptedException {
		zooKeeper.create(nodePath,nodeValue.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
	
	public String  getzNode(ZooKeeper zooKeeper,String nodePath) throws KeeperException, InterruptedException {
		byte[] data = zooKeeper.getData(nodePath, false, new Stat());
		String dataString = new String(data);
		return dataString;
	}
	
	
}
