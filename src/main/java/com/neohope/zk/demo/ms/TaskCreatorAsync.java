package com.neohope.zk.demo.ms;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;

/**
 * TaskCreator的异步版本
 * 创建任务，监视任务状态
 * 任务完成后，删除任务
 */
class TaskCreatorAsync implements Watcher, Closeable{
	private final static Logger logger = LoggerFactory.getLogger(TaskCreatorAsync.class);
	
	ZooKeeper zk;
	String address;
	
	volatile boolean connected = false;
    volatile boolean expired = false;
	
	Random rand=new Random();
	String clientId=rand.nextLong()+"";
	
	ConcurrentHashMap<String, Object> ctxMap = new ConcurrentHashMap<>();
	
	TaskCreatorAsync(String address){
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
	
	 boolean isConnected(){
	 	return connected;
	 }
	 
	 boolean isExpired(){
		return expired;
	 }
	
	/**
	 * Watcher回调函数
	 */
	public void process(WatchedEvent e) {
		logger.info(e.toString());
        if(e.getType() == EventType.None){
            switch (e.getState()) {
            case SyncConnected:
                connected = true;
                break;
            case Disconnected:
                connected = false;
                break;
            case Expired:
                expired = true;
                connected = false;
                logger.error("Exiting due to session expiration");
            default:
                break;
            }
        } else if(e.getType() == EventType.NodeCreated) {
        	if(e.getPath()!=null && e.getPath().startsWith("/status/task-"))
			{
        		if(ctxMap.containsKey( e.getPath())){
        			zk.getData(e.getPath(), false, getDataCallback, ctxMap.get(e.getPath()));
        		}
			}
        }
	}
	
	/**
	 * 任务提交
	 */
	void submitTask(String task){
		zk.create("/tasks/task-", task.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL, createTaskCallback, task);
	}
	
    StringCallback createTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
            	submitTask((String)ctx);
                break;
            case OK:
                logger.info("Created task name: "+name);
                watchStatus("/status/"+name.replaceAll("/tasks/", ""),ctx);
                break;
            default:
            	logger.error("createTaskCallback Unsported OP");
            	break;
            }
        }
    };
    
    /**
	 * 监视任务状态
	 */
    void watchStatus(String path, Object ctx){
    	ctxMap.put(path, ctx);
    	zk.exists(path, this, existsCallback, ctx);
    }
    
    private StatCallback existsCallback = new StatCallback(){
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch(Code.get(rc))
			{
			case CONNECTIONLOSS:
				watchStatus(path, ctx);
				break;
			case OK:
				if(stat!=null){
					zk.getData(path, false, getDataCallback, null);
				}
				break;
			case NONODE:
				break;
			default:
				logger.error("existCallback Unsported OP");
				break;
			}
		}
	};
	
    /**
	 * 监视任务状态
	 */
	DataCallback getDataCallback = new DataCallback(){
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                zk.getData(path, false, getDataCallback, ctxMap.get(path));
                break;
            case OK:
                String taskResult = new String(data);
                logger.info("Task " + path + ", " + taskResult);

                assert(ctx != null);
                zk.delete(path, -1, taskDeleteCallback, null);
                ctxMap.remove(path);
                break;
            case NONODE:
                logger.warn("Status node is gone!");
                break; 
            default:
            	logger.error("getDataCallback Unsported OP");
				break;              
            }
        }
    };
    
    /**
	 * 监视任务删除
	 */
    VoidCallback taskDeleteCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                zk.delete(path, -1, taskDeleteCallback, null);
                break;
            case OK:
                logger.info("Successfully deleted " + path);
                break;
            default:
            	logger.error("taskDeleteCallback Unsported OP");
				break;  
            }
        }
    };
	
	public static void main(String args[]) throws IOException {
		TaskCreatorAsync m=new TaskCreatorAsync("localhost:2181");
		m.startZk();
		m.submitTask("T"+m.rand.nextInt());
		logger.info(">>>>>>new task created");
		m.close();
	}	
	
}