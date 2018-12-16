package com.neohope.zk.demo.ms;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.Random;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 枚举任务信息
 */
class StateReporter implements Watcher, Closeable{
	
	private final static Logger logger = LoggerFactory.getLogger(StateReporter.class);
			
	ZooKeeper zk;
	String address;
	
	Random rand=new Random();
	String reporterId=rand.nextLong()+"";
	
	StateReporter(String address){
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
	
	public void process(WatchedEvent e) {
		logger.info(e.toString());
	}
	
	/**
	 * 枚举任务信息
	 */
	void listState() throws KeeperException, InterruptedException{
		try {
			Stat stat = new Stat();
			byte masterData[] = zk.getData("/master", false, stat);
			Date startDate = new Date(stat.getCtime());
			logger.info(">>>>>>Master: "+new String(masterData) + " since "+startDate);
		}catch(NoNodeException e) {
			logger.warn(">>>>>>No Master");
		}
		
		logger.info(">>>>>>Workers:");
		for(String w:zk.getChildren("/workers", false)) {
			byte data[] = zk.getData("/workers/"+w, false, null);
			String state = new String(data);
			logger.info("\t"+w+": "+state);
		}
		
		logger.info(">>>>>>Tasks:");
		for(String t: zk.getChildren("/tasks", false)) {
			logger.info("\t"+t);
		}
		
		logger.info(">>>>>>Assign:");
		for(String t: zk.getChildren("/assign", false)) {
			logger.info("\t"+t);
		}
	}
	
	public static void main(String args[]) throws IOException, KeeperException, InterruptedException {
		StateReporter c=new StateReporter("localhost:2181");
		c.startZk();
		c.listState();
		c.close();
	}
	
	
}