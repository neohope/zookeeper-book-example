package com.neohope.zk.demo.ms;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neohope.zk.demo.utils.ChildrenCache;
import com.neohope.zk.demo.utils.IRecoveryCallback;
import com.neohope.zk.demo.utils.RecoveredAssignments;
import com.neohope.zk.demo.utils.ZkTask;

/**
 * Master的异步版本
 * 1、竞选Master，如果没有被选上，则等待被选上
 * 2、如果选上了，则初始化节点，重新分配任务，并监听每个节点的变化
 * 3、Task节点变化时，尝试分配任务，初始化任务状态
 * 4、Worker节点变化时，尝试恢复任务，并取消任务分配，取消任务状态
 * 5、挂掉时，会主动释放Master节点
 */
class MasterAsync implements Watcher, Closeable{
	
	private final static Logger logger = LoggerFactory.getLogger(MasterAsync.class);
	
	Random rand=new Random();
	String serverId="master"+rand.nextLong();
	
	ZooKeeper zk;
	String address;
	
	boolean isLeader=false;
	private volatile boolean connected = false;
    private volatile boolean expired = false;
	
	ChildrenCache workersCache;
	ChildrenCache tasksCache;
	
	MasterAsync(String address){
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
	
	boolean isConnected() {
        return connected;
    }
    
    boolean isExpired() {
        return expired;
    }
	
	/**
	 * 竞选到Master之后，初始化系统所需节点
	 */
	private void initNode(byte[] data) throws InterruptedException{
		zk.create("/workers", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, initNodeCallback, null);
		zk.create("/tasks", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, initNodeCallback, null);
		zk.create("/status", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, initNodeCallback, null);
		zk.create("/assign", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, initNodeCallback, null);
	}
	
	StringCallback initNodeCallback = new StringCallback(){
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch(Code.get(rc))
			{
			case CONNECTIONLOSS:
				try {
					initNode(new byte[0]);
				} catch (InterruptedException e) {
					logger.warn(e.getMessage());
				}
				break;
			case NODEEXISTS:
				logger.info("Node exists: " +  path);
				break;
			case OK:
				logger.info("Node created: " +  path);
				break;
			default:
				logger.warn("Some thing is wrong:" + Code.get(rc)+" "+path);
			}
		}
	};
	
	/**
	 * 竞选Master，如果没有选上，就等待master节点消失，再选
	 */
	private void runForMaster() throws InterruptedException{
		zk.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, 
				CreateMode.EPHEMERAL, masterCreateCallback, null);
		if(isLeader)initNode(new byte[0]);
	}
	
	StringCallback masterCreateCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                checkMaster();
                break;
            case OK:
            	isLeader=true;
                takeLeadership();
                break;
            case NODEEXISTS:
            	isLeader=false;
                masterExists();
                break;
            default:
            	isLeader=false;
            	logger.warn("masterCreateCallback is wrong:" + Code.get(rc)+" "+path);
            }
            
            logger.info("I'm " + (isLeader? "" : "not ") + "the leader " + serverId);
        }
    };
	
	/**
	 * 竞选Master成功后
	 * 遍历节点，对于已挂掉的worker的任务分配
     * 重新建立任务，删除任务分配和任务状态
	 */
	void takeLeadership() {
        logger.info("Going for list of workers");
        getWorkers();
        
        (new RecoveredAssignments(zk)).recover(new IRecoveryCallback() {
            public void recoveryComplete(int rc, List<String> tasks) {
                if(rc == IRecoveryCallback.FAILED) {
                    logger.error("Recovery of assigned tasks failed.");
                } else {
                	logger.info( "Assigning recovered tasks" );
                    getTasks();
                }
            }
        });
    }
	
	/**
	 * master节点是否存在
	 */
	void masterExists() {
        zk.exists("/master", this, masterExistsCallback, null);
    }
    
    StatCallback masterExistsCallback = new StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                masterExists();
                break;
            case OK:
                break;
            case NONODE:
                isLeader = false;
                try {
					runForMaster();
				} catch (InterruptedException e) {
					logger.warn(e.getMessage());
				}
                logger.info("It sounds like the previous master is gone, " +
                    		"so let's run for master again."); 
                break;
            default:     
                checkMaster();
                break;
            }
        }
    };
	
    /**
	 * 检查是否为Master
	 */
	private void checkMaster(){
		zk.getData("/master", false, masterCheckCallback, null);
	}
	
	DataCallback masterCheckCallback = new DataCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch(Code.get(rc))
			{
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case NONODE:
				try {
					runForMaster();
				} catch (InterruptedException e) {
					logger.warn(e.getMessage());
				}
				break;
			case NODEEXISTS:
				waitToBeMaster();
				break;
			case OK:
				if( serverId.equals( new String(data) ) ) {
					isLeader=true;
                    takeLeadership();
                } else {
                	isLeader=false;
                    masterExists();
                }
				
				break;
			default:
				isLeader=false;
			}
			
			logger.info(">>>>>>I'am "+(isLeader?"":"not ") + "the leader");
		}
	};
	
	/**
	 * Watcher回调函数，根据Path，拆分到不同函数进行后续处理
	 */
	public void process(WatchedEvent e) {
		logger.info("Processing event: " + e.toString());
        if(e.getType() == Event.EventType.None){
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
                logger.error("Session expiration");
            default:
                break;
            }
        } else if(e.getPath()!=null){
			if(e.getPath().startsWith("/master")){
				processMaster(e);
			}
			else if(e.getPath().startsWith("/workers")){
				processWorkers(e);
			}
			else if(e.getPath().startsWith("/tasks")){
				processTasks(e);
			}
		}
		else{
			logger.warn(e.toString());
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
			getWorkers();
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
			getTasks();
			logger.info("Task Children Changed");
			break;
		default:
			logger.info("Task Unsported OP");
			break;
		}
	}
	
	/**
	 * 等待其他Master节点退出，然后竞选Master
	 */
	private void waitToBeMaster(){
		zk.exists("/master", this, masterExistCallback, null);
	}
	
	private StatCallback masterExistCallback = new StatCallback(){
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch(Code.get(rc))
			{
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case NONODE:
				try {
					runForMaster();
				} catch (InterruptedException e) {
					logger.warn(e.getMessage());
				}
				break;
			
			default:
				isLeader=false;
			}
		}
	};
	
	/**
	 * 获取worker数量
	 */
	public int getWorkersSize(){
        if(workersCache == null) {
            return 0;
        } else {
            return workersCache.getList().size();
        }
    }
	
	/**
	 * 获取worker列表，如果有worker挂掉，要重新分配任务
	 */
	void getWorkers(){
		zk.getChildren("/workers", this, workersGetChidrenCallback, null);
	}
	
	ChildrenCallback workersGetChidrenCallback = new ChildrenCallback(){
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			switch(Code.get(rc))
			{
			case CONNECTIONLOSS:
				getWorkers();
				break;
			case OK:
				reassignAndSet(children);
				break;
			default:
				logger.error("worsGetChidrenCallback Unsported OP");
			}
		}
		
	};
	
	/**
	 * 如果有worker挂掉，要重新分配任务
	 */
	void reassignAndSet(List<String> children){
		List<String> toProcess;
		
		if(workersCache==null){
			workersCache=new ChildrenCache(children);
			toProcess=null;
		}else{
			toProcess=workersCache.setAndGetOnlyInOld(children);
		}
		
		if(toProcess!=null){
			for(String worker:toProcess){
				getAbsentWorkerTasks(worker);
			}
		}
		
		getTasks();
	}
	
	/**
	 * 获取任务分配列表，重新进行任务分配
	 */
	void getAbsentWorkerTasks(String worker){
        zk.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
    }
    
    ChildrenCallback workerAssignmentCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getAbsentWorkerTasks(path);
                break;
            case OK:
                logger.info("Succesfully got a list of assignments: " + children.size() + " tasks");
                for(String task: children) {
                    getDataReassign(path + "/" + task, task);                    
                }
                break;
            default:
            	logger.error("workerAssignmentCallback Unsported OP");
            }
        }
    };
    
	/**
	 * 获取重新分配任务的数据
	 */
    void getDataReassign(String path, String task) {
        zk.getData(path, false, getDataReassignCallback, task);
    }
    
    DataCallback getDataReassignCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                getDataReassign(path, (String) ctx);
                break;
            case OK:
                recreateTask(new ZkTask(path, (String) ctx, data));
                break;
            default:
                logger.error("getDataReassignCallback Unsported OP");
            }
        }
    };
    
	/**
	 * 重新添加已分配任务，删除任务分配
	 */
    void recreateTask(ZkTask ctx) {
        zk.create("/tasks/" + ctx.task, ctx.data,Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT, recreateTaskCallback, ctx);
    }
    
    StringCallback recreateTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                recreateTask((ZkTask) ctx);
                break;
            case OK:
                deleteAssignment(((ZkTask) ctx).path);
                break;
            case NODEEXISTS:
                logger.info("Node exists already, but if it hasn't been deleted, " +
                		"then it will eventually, so we keep trying: " + path);
                recreateTask((ZkTask) ctx);
                break;
            default:
            	logger.error("recreateTaskCallback Unsported OP");
            }
        }
    };
    
	/**
	 * 删除任务分配
	 */
    void deleteAssignment(String path){
        zk.delete(path, -1, taskDeletionCallback, null);
    }
    
    VoidCallback taskDeletionCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteAssignment(path);
                break;
            case OK:
                logger.info("Task correctly deleted: " + path);
                break;
            default:
            	logger.error("taskDeletionCallback Unsported OP");
            } 
        }
    };
	
	/**
	 * 获取任务列表，对于每一个任务，获取节点数据， 并分配任务
	 */
	void getTasks(){
		zk.getChildren("/tasks", this, tasksGetChildrenCallback,null);
	}
	
	ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback(){
		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			switch(Code.get(rc))
			{
			case CONNECTIONLOSS:
				getTasks();
				break;
			case OK:
				List<String> toProcess;
                if(tasksCache == null) {
                    tasksCache = new ChildrenCache(children);
                    toProcess = children;
                } else {
                    toProcess = tasksCache.setAndGetBothOldAndNew(children);
                }
                
                if(toProcess != null){
                    assignTasks(toProcess);
                } 
				break;
			
			default:
				logger.error("taskGetChildrenCallback Unsported OP");
			}
		}
	};
	
	void assignTasks(List<String> tasks){
		for(String t:tasks){
			getTaskData(t);
		}
	}
	
	/**
	 * 获取任务数据，分配任务，并删除任务节点
	 */
	void getTaskData(String task){
		zk.getData("/tasks/"+task, false, taskDataCallback,task);
	}
	
	DataCallback taskDataCallback = new DataCallback(){
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch(Code.get(rc))
			{
			case CONNECTIONLOSS:
				getTaskData((String)ctx);
				break;
			case OK:
				List<String> list = workersCache.getList();
				if(list!=null && list.size()>0){
					int workerId=rand.nextInt(list.size());
					String designatedWorker = list.get(workerId);
					String assignmentPath = "/assign/"+designatedWorker+"/"+(String)ctx;
					createAssignment(assignmentPath,data);
				}
				else{
					logger.info("no worker found");
				}
				break;
			
			default:
				logger.error("taskDataCallback Unsported OP");
			}
		}
	};
	
	/**
	 * 创建分配任务节点，并删除任务节点
	 */
	void createAssignment(String path, byte[] data){
		zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, assignTaskCallback, data);
	}
	
	StringCallback assignTaskCallback = new StringCallback(){
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch(Code.get(rc))
			{
			case CONNECTIONLOSS:
				createAssignment(path, (byte[])ctx);
				break;
			case OK:
				logger.info("Task assigned: "+name);
				deleteTask(name.substring(name.lastIndexOf("/")+1));
				break;
			case NODEEXISTS:
				logger.info("Task already assigned");
				break;
			default:
				logger.error("taskDataCallback Unsported OP");
			}
		}
	};
	
	/**
	 * 删除任务
	 */
	void deleteTask(String name){
        zk.delete("/tasks/" + name, -1, taskDeleteCallback, null);
    }
    
    VoidCallback taskDeleteCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteTask(path);
                break;
            case OK:
                logger.info("task deleted " + path);
                break;
            case NONODE:
            	logger.info("Task has been deleted");
                break;
            default:
            	logger.error("taskDeleteCallback Unsported OP");
            }
        }
    };
	
	public static void main(String args[]) throws IOException, KeeperException, InterruptedException {
		MasterAsync m=new MasterAsync("localhost:2181");
		m.startZk();
		m.runForMaster();
		
		logger.info(">>>>>>Master started, press enter to exit");
		System.in.read();
		logger.info(">>>>>>Master exited");
	
		m.close();
	}
}