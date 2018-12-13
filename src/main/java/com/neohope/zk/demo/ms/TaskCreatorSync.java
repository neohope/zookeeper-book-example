package com.neohope.zk.demo.ms;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooKeeper;

/**
 * TaskCreator的同步版本，功能少于异步版本
 * 创建Task然后退出
 */
class TaskCreatorSync implements Watcher, Closeable{
	private final static Logger logger = LoggerFactory.getLogger(TaskCreatorSync.class);
	
	ZooKeeper zk;
	String address;
	
	Random rand=new Random();
	String clientId=rand.nextLong()+"";
	
	TaskCreatorSync(String address){
		this.address=address;
	}
	
	void startZk() throws IOException {
		zk = new ZooKeeper(address,15000,this);
	}
	
	@Override
	public void close() throws IOException {
		try {
			zk.close();
		} catch (InterruptedException e) {
			logger.warn(e.getMessage());
		}
	}
	
	/**
	 * Watcher回调函数
	 */
	public void process(WatchedEvent e) {
		logger.info(e.toString());
		if(e.getType() == EventType.None){
            switch (e.getState()) {
            case SyncConnected:
                break;
            case Disconnected:
                break;
            case Expired:
                logger.error("Exiting due to session expiration");
            default:
                break;
            }
        }
	}
	
	/**
	 * 创建task
	 */
	public String createTask(String command) {
		try {
			String name=zk.create("/tasks/task-", command.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			return name;
		}catch(NodeExistsException e){
			logger.warn(e.getMessage());
		}catch(ConnectionLossException e){
			logger.warn(e.getMessage());
		} catch (KeeperException e) {
			logger.warn(e.getMessage());
		} catch (InterruptedException e) {
			logger.warn(e.getMessage());
		}
		
		return "error";
	}
	
	public static void main(String args[]) throws IOException {
		TaskCreatorSync m=new TaskCreatorSync("localhost:2181");
		m.startZk();
		m.createTask("");
		logger.info(">>>>>>new task created");
		m.close();
	}
	
	
}