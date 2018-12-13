package com.neohope.zk.demo.ms;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neohope.zk.demo.utils.ChildrenCache;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;

/**
 * Worker的异步版本
 * 1、注册worker，注册assign
 * 2、监听assign节点变化
 * 3、新任务分配后，处理任务，修改任务状态，删除任务分配
 */
class WorkerAsync implements Watcher, Closeable{
	private final static Logger logger = LoggerFactory.getLogger(WorkerAsync.class);
	
	ZooKeeper zk;
	String address;
	String status;
	
	Random rand=new Random();
	String workerId=rand.nextLong()+"";
	
	private volatile boolean connected = false;
	private volatile boolean expired = false;
	
	private ThreadPoolExecutor executor;
	private ChildrenCache assignedTasksCache = new ChildrenCache();
	
	WorkerAsync(String address){
		this.address=address;
		this.status="";
		this.executor = new ThreadPoolExecutor(1, 1, 1000L, TimeUnit.MILLISECONDS, 
				new ArrayBlockingQueue<Runnable>(200));
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
	
	public boolean isConnected() {
        return connected;
    }
	
    public boolean isExpired() {
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
                logger.error("Session expired");
            default:
                break;
            }
        }
		else if(e.getType() == EventType.NodeChildrenChanged) {
			if(e.getPath()!=null && e.getPath().startsWith("/assign/worker-"+workerId)){
				getAssign();
            }
		}
	}
	
	/**
	 * worker注册
	 */
	public void regist() {
		zk.create("/workers/worker-"+workerId, "Idle".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, registCallback, null);
	}
	
	StringCallback registCallback = new StringCallback(){
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch(Code.get(rc))
			{
			case CONNECTIONLOSS:
				regist();
				break;
			case OK:
				logger.info("Worker created: " +  path);
				break;
			case NODEEXISTS:
				logger.info("Worker registered: " +  path);
				break;
			default:
				logger.warn("Some thing is wrong:" + Code.get(rc)+" "+path);
			}
		}
	};
	
	/**
	 * worker assign 节点注册
	 */
	void createAssignNode(){
        zk.create("/assign/worker-" + workerId, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                createAssignCallback, null);
    }
	
	StringCallback createAssignCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                createAssignNode();
                break;
            case OK:
                logger.info("Assign node created");
                break;
            case NODEEXISTS:
            	logger.warn("Assign node already registered");
                break;
            default:
            	logger.error("Something went wrong: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };
	
	/**
	 * 更新worker状态
	 */
	public void setStatus(String status){
		this.status=status;
		updateStatus(status);
	}
	
	synchronized private void updateStatus(String status){
		if(status==this.status){
			zk.setData("/works/"+workerId, status.getBytes(), -1, statusUpdateCallback, status);
		}
	}
	
	StatCallback statusUpdateCallback = new StatCallback(){
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch(Code.get(rc))
			{
			case CONNECTIONLOSS:
				updateStatus((String)ctx);
				break;
			default:
				logger.warn("Some thing is wrong:" + Code.get(rc)+" "+path);
			}
			
		}
	};
	
	/**
	 * 更新worker状态
	 * 完成任务后，更新任务，删除任务分配
	 */
	void getAssign(){
		zk.getChildren("/assign/worker-"+workerId, this, assignGetChildrenCallback,null);
	}
	
	ChildrenCallback assignGetChildrenCallback = new ChildrenCallback(){
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			switch(Code.get(rc))
			{
			case CONNECTIONLOSS:
				getAssign();
				break;
			case OK:
				if(children!=null){
					executor.execute(new Runnable(){
						List<String> children;
						DataCallback cb;
						
						public Runnable init(List<String> children, DataCallback cb){
							this.children=children;
							this.cb=cb;
							return this;
						}
						
						public void run(){
							if(children == null)return;
							logger.info("Looping ito tasks");
							for(String task:children){
								logger.info("New task: {}", task);
							    zk.getData("/assign/worker-"+workerId+"/"+task, false, cb,task);
							}
						}
					}.init(assignedTasksCache.setAndGetBothOldAndNew(children), taskDataCallback));
				}
				break;
			
			default:
				logger.error("assignGetChildrenCallback Unsported OP");
			}
		}
	};
	
	/**
	 * 更新任务，删除任务分配
	 */
	DataCallback taskDataCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                zk.getData(path, false, taskDataCallback, null);
                break;
            case OK:
                executor.execute( new Runnable() {
                    byte[] data;
                    Object ctx;
                    
                    public Runnable init(byte[] data, Object ctx) {
                        this.data = data;
                        this.ctx = ctx;
                        return this;
                    }
                    
                    public void run() {
                        logger.info("Executing your task: " + new String(data));
                        try {
							Thread.sleep(3000);
						} catch (InterruptedException e) {
						}
                        
                        zk.create("/status/" + (String) ctx, "done".getBytes(), Ids.OPEN_ACL_UNSAFE, 
                                CreateMode.PERSISTENT, taskStatusCreateCallback, null);
                        zk.delete("/assign/worker-" + workerId + "/" + (String) ctx, 
                                -1, taskVoidCallback, null);
                    }
                }.init(data, ctx));
                
                break;
            default:
            	logger.warn("taskDataCallback nsported OP:" + Code.get(rc)+" "+path);
            }
        }
    };
    
	/**
	 * 更新任务状态CB
	 */
    StringCallback taskStatusCreateCallback = new StringCallback(){
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                zk.create(path + "/status", "done".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                        taskStatusCreateCallback, null);
                break;
            case OK:
                logger.info("Created status znode correctly: " + name);
                break;
            case NODEEXISTS:
            	logger.warn("Node exists: " + path);
                break;
            default:
            	logger.warn("taskStatusCreateCallback nsported OP:" + Code.get(rc)+" "+path);
            }
        }
    };
    
	/**
	 * 删除任务CB
	 */
    VoidCallback taskVoidCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                break;
            case OK:
                logger.info("Task correctly deleted: " + path);
                break;
            default:
            	logger.warn("taskVoidCallback nsported OP:" + Code.get(rc)+" "+path);
            } 
        }
    };
	
	public static void main(String args[]) throws IOException {
		WorkerAsync m=new WorkerAsync("localhost:2181");
		m.startZk();
		m.regist();
		m.createAssignNode();
		m.getAssign();
		
		logger.info(">>>>>>Worker started, press enter to exit");
		System.in.read();
		logger.info(">>>>>>Worker ended");
		m.close();
	}
	
	
}