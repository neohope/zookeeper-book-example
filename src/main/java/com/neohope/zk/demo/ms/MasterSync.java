package com.neohope.zk.demo.ms;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Master的同步版本，功能少于异步版本
 * 1、竞选Master，如果没有被选上，则等待被选上
 * 2、如果选上了，则初始化节点
 * 3、挂掉时，会主动释放Master节点
 */
class MasterSync implements Watcher, Closeable{
	
	private final static Logger logger = LoggerFactory.getLogger(MasterSync.class);
	
	ZooKeeper zk;
	String address;
	
	boolean isLeader=false;
	Random rand=new Random();
	String serverId=rand.nextLong()+"";
	
	MasterSync(String address){
		this.address=address;
	}
	
	private void startZk() throws IOException, InterruptedException {
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
	 * 竞选Master，如果没有选上，就等待master节点消失，再选
	 */
	private void runForMaster() throws InterruptedException
	{
		try {
			zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (KeeperException e) {
			logger.warn(e.getMessage());
		}
		checkMaster();
		if(!isLeader)initNode();
	}
	
	private void checkMaster(){
		try {
			Stat stat = new Stat();
			byte [] masterData = zk.getData("/master", false, stat);
			isLeader = new String(masterData).equals(serverId);
		} catch (KeeperException | InterruptedException e) {
			logger.warn(e.getMessage());
			isLeader=false;
		}
	}
	
	/**
	 * 竞选到Master之后，初始化系统所需节点
	 */
	private void initNode() throws InterruptedException{
		try {
			zk.create("/workers", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			logger.warn(e.getMessage());
		}
		
		try {
			zk.create("/tasks", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			logger.warn(e.getMessage());
		}
		
		try {
			zk.create("/status", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			logger.warn(e.getMessage());
		}
		
		try {
			zk.create("/assign", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			logger.warn(e.getMessage());
		}
	}
	
	/**
	 * Watcher回调函数，根据Path，拆分到不同函数进行后续处理
	 */
	public void process(WatchedEvent e) {
		if(e.getPath()!=null){
			if(e.getPath().startsWith("/master")){
				processMaster(e);
			}
			else if(e.getPath().startsWith("/works")){
				processWorkers(e);
			}
			else if(e.getPath().startsWith("/tasks")){
				processTasks(e);
			}
		}
		else{
			logger.info(e.toString());
		}
	}
	
	private void processMaster(WatchedEvent e){
		switch(e.getType()){
		case NodeCreated:
			logger.info("Master Added");
			break;
		case NodeDeleted:
			logger.info("Master Removed");
			try {
				runForMaster();
			} catch (InterruptedException ex) {
				logger.warn(ex.getMessage());
			}
			break;
		case NodeDataChanged:
			logger.info("Master Changed");
			break;
		case NodeChildrenChanged:
			logger.info("Master Children Changed");
			break;
		default:
			logger.info("Master Unsported OP");
			break;
		}
	}
	
	private void processWorkers(WatchedEvent e){
		switch(e.getType()){
		case NodeCreated:
			logger.info("Worker Added");
			break;
		case NodeDeleted:
			logger.info("Worker Removed");
			break;
		case NodeDataChanged:
			logger.info("Worker Changed");
			break;
		case NodeChildrenChanged:
			logger.info("Worker Children Changed");
			break;
		default:
			logger.info("Worker Unsported OP");
			break;
		}
	}
	
	private void processTasks(WatchedEvent e){
		switch(e.getType()){
		case NodeCreated:
			logger.info("Task Added");
			break;
		case NodeDeleted:
			logger.info("Task Removed");
			break;
		case NodeDataChanged:
			logger.info("Task Changed");
			break;
		case NodeChildrenChanged:
			logger.info("Task Children Changed");
			break;
		default:
			logger.info("Task Unsported OP");
			break;
		}
	}
	
	public static void main(String args[]) throws IOException, KeeperException, InterruptedException {
		MasterSync m=new MasterSync("localhost:2181");
		m.startZk();
		while(!m.isLeader){
			m.runForMaster();
			Thread.sleep(500);
			logger.info(">>>>>>I'am "+(m.isLeader?"":"not ") + "the leader");
		}
		
		logger.info(">>>>>>Master started, press enter to exit");
		System.in.read();
		logger.info(">>>>>>Master exited");
		m.close();
	}
}