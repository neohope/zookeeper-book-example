package com.neohope.zk.demo.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry; 
 
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neohope.zk.demo.utils.RecoveredAssignments;
import com.neohope.zk.demo.utils.IRecoveryCallback;

/**
 * Master的Curator LeaderSelectorListener版本
 * 1、竞选Master，如果没有被选上，则等待被选上
 * 2、如果选上了，则初始化节点，重新分配任务，并监听每个节点的变化
 * 3、Task节点变化时，尝试分配任务，初始化任务状态
 * 4、Worker节点变化时，尝试恢复任务，并取消任务分配，取消任务状态
 * 5、挂掉时，会主动释放Master节点
 */
public class MasterCuratorSelector implements Closeable, LeaderSelectorListener{
    private static final Logger logger = LoggerFactory.getLogger(MasterCuratorSelector.class);
    
	Random rand=new Random(System.currentTimeMillis());
	String masterId="master"+rand.nextLong();
	
    private CuratorFramework client;
    
    private final LeaderSelector leaderSelector;
    private final PathChildrenCache workersCache;
    private final PathChildrenCache tasksCache;
    private final PathChildrenCache assignCache;
    private final Map<String, PathChildrenCache> assignTaskMap;
    
    //等待被选为Leader
    private CountDownLatch leaderLatch = new CountDownLatch(1);
    //保证Master不退出
    private CountDownLatch closeLatch = new CountDownLatch(1);
    //重新分配任务后，再监听任务变化
    CountDownLatch recoveryLatch = new CountDownLatch(0);
    
    public MasterCuratorSelector(String hostPort, RetryPolicy retryPolicy){
        this.client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy);
        this.leaderSelector = new LeaderSelector(this.client, "/master", this);
        this.workersCache = new PathChildrenCache(this.client, "/workers", true);
        this.tasksCache = new PathChildrenCache(this.client, "/tasks", true);
        this.assignCache = new PathChildrenCache(this.client, "/assign", true);
        this.assignTaskMap=new HashMap<>();
    }
    
    public void startZK() {
        client.start();
    }
    
    @Override
    public void close() throws IOException {
        logger.info( "Closing" );
        closeLatch.countDown();
        leaderSelector.close();
        client.close();
    }
    
    public void awaitLeadership() throws InterruptedException {
    	leaderLatch.await();
    }
    
	/**
	 * 竞选到Master之后，初始化系统所需节点
	 */
    public void initNode() {
    	createNode("/workers", new byte[0]);
    	createNode("/assign", new byte[0]);
    	createNode("/tasks", new byte[0]);
    	createNode("/status", new byte[0]);
    }
    
    private void createNode(String path, byte[] data) {
    	try {
			client.create().forPath(path, new byte[0]);
		} catch (Exception e) {
			logger.info(path + " exists");
		}
    }
    
	/**
	 * 竞选Master
	 */
    public void runForMaster() {
    	logger.info( ">>>>>>Starting master selection: " + masterId);
        leaderSelector.setId(masterId);
        leaderSelector.start();
    }
    
	/**
	 * Leadership回调函数
	 */
    @Override
    public void takeLeadership(CuratorFramework client) throws Exception
    {   
        logger.info( ">>>>>>Mastership participants: " + masterId + ", " + leaderSelector.getParticipants() );
        
        //开始监视worker
        workersCache.getListenable().addListener(workersCacheListener);
        workersCache.start();
        
        //处理挂掉的worker，重新分配任务
        (new RecoveredAssignments(client.getZookeeperClient().getZooKeeper())).recover( new IRecoveryCallback() {
            public void recoveryComplete (int rc, List<String> tasks) {
                try{
                	List<ChildData> workersList = workersCache.getCurrentData(); 
                    if(rc == IRecoveryCallback.FAILED) {
                        logger.warn("Recovery of assigned tasks failed.");
                    } else {
                        logger.info( "Assigning recovered tasks" );
                        recoveryLatch = new CountDownLatch(tasks.size());
                        if(!workersList.isEmpty())assignTasks(tasks);
                    }
                    
                    new Thread( new Runnable() {
                        public void run() {
                            try{
                            //等待处理完毕
                            if(tasks.size()>0 && !workersList.isEmpty())recoveryLatch.await();
                            logger.info("RecoveredAssignments finished");
                            
                            //开始监听tasks
                            tasksCache.getListenable().addListener(tasksCacheListener);
                            tasksCache.start();
                            
                             //开始监听assign
                            assignCache.getListenable().addListener(assignCacheListener);
                            assignCache.start();
                            } catch (Exception e) {
                                logger.warn("Exception while assigning and getting tasks.", e  );
                            }
                        }
                    }).start();
                    
                    leaderLatch.countDown();
                } catch (Exception e) {
                    logger.error("Exception while executing the recovery callback", e);
                }
            }
        });
        
        List<ChildData> workersList = workersCache.getCurrentData();
        for(ChildData workerCache :workersList){
        	String worker=workerCache.getPath().replaceFirst("/workers/", "");
            String path = "/assign/" + worker;
			PathChildrenCache assignTaskCache=new PathChildrenCache(client, path, true); 
	    	assignTaskCache.getListenable().addListener(assignTaskCacheListener);
			assignTaskCache.start();
			assignTaskMap.put(worker, assignTaskCache);
        }

        closeLatch.await();
    }
    
	/**
	 * 状态变化回调函数
	 */
    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        switch(newState){
        case CONNECTED:
            break;
        case RECONNECTED:
            break;
        case SUSPENDED:
            logger.warn("Session suspended");
            break;
        case LOST:
            try{
                close();
            } catch (IOException e) {
                logger.warn( "Exception while closing", e );
            }
            break;
        case READ_ONLY:
            break;
        }
    }
    
    /**
     * worker挂掉后，获取已分配任务，并重新分配
     */
    PathChildrenCacheListener workersCacheListener = new PathChildrenCacheListener() {
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
            if(event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                try{
                    getAbsentWorkerTasks(event.getData().getPath().replaceFirst("/workers/", ""));
                } catch (Exception e) {
                    logger.error("Exception while trying to re-assign tasks", e);
                }
            } else if(event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
            	//有新worker后，尝试分配Task
                logger.info("new worker connected: " + event.getType());
                try {
					getTasksAndAssign();
				} catch (Exception e) {
					logger.warn("getTasksAndAssign error: "+e.getMessage());
				}
            }
        }
    };
    
    /**
     * 获取某个已经挂掉的worker已分配到的任务，然后进行回收处理
     */
    private void getAbsentWorkerTasks(String worker) throws Exception { 
        List<String> tasks = client.getChildren().forPath("/assign/" + worker); 
        
    	logger.info("Succesfully got a list of assignments: " + tasks.size() + " tasks");
		//删除assign/worker/task节点
		for(String task : tasks){
		  deleteAssignment("/assign/" + worker + "/" + task);
		}
		
		//删除assign/worker节点
		deleteAssignment("/assign/" + worker);
		
		//重新分配任务
		assignTasks(tasks);
    }
    
    /**
     * 删除任务分配
     */
    void deleteAssignment(String path) throws Exception {
        logger.info( "Deleting assignment: {}", path );
        client.delete().inBackground().forPath(path);
    }

    /**
     * 重新分配任务列表
     */
    void assignTasks(List<String> tasks) {
      for(String task : tasks) {
    	byte[] data=new byte[0];
		try {
			data = client.getData().forPath("/tasks/" + task);
		} catch (Exception e) {
			logger.warn("client.getData() error: "+e.getMessage());
		}
        assignTask(task, data);
      }
    }

    /**
     * 随机选择worker，分配任务
     */
    void assignTask (String task, byte[] data){
    	logger.info("Assigning task {}, data {}", task, new String(data));
        List<ChildData> workersList = workersCache.getCurrentData(); 
        if(workersList.isEmpty()){
        	logger.info("No worker founded for " + task);
        	return;
        }
        
        String designatedWorker = workersList.get(rand.nextInt(workersList.size())).getPath().replaceFirst("/workers/", "");
        String path = "/assign/" + designatedWorker + "/" + task;
        createAssignment(path, data);
    }

    /**
     * 创建任务分配
     */
    void createAssignment(String path, byte[] data){
        try {
			client.create().withMode(CreateMode.PERSISTENT).inBackground().forPath(path, data);
		} catch (Exception e) {
			logger.warn("createAssignment error: "+e.getMessage());
		} 
    }
    
    /**
     * 删除任务
     */
    void deleteTask(String number) throws Exception {
        logger.info("Deleting task: {}", number);
        client.delete().inBackground().forPath("/tasks/task-" + number);
        recoveryLatch.countDown();
    }
    
    /**
     * 如果还有任务，重新分配任务
     */
    void getTasksAndAssign() throws Exception {
        logger.info("getTasksAndAssign");
        List<String> tasks = client.getChildren().forPath("/tasks");
        
        if(tasks==null)return;
        for(String task:tasks){
        	byte[] data = client.getData().forPath("/tasks/"+task);
        	assignTask(task, data);
        }
    }
    
    /**
     * 收到任务后，分配任务到worker
     */
    PathChildrenCacheListener tasksCacheListener = new PathChildrenCacheListener() {
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
            if(event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
            	//分配任务
                try{
                    assignTask(event.getData().getPath().replaceFirst("/tasks/", ""),
                            event.getData().getData());
                } catch (Exception e) {
                    logger.error("Exception when assigning task.", e);
                }   
            }else if(event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
            	//删除任务
                logger.info("Task Finished: " + event.getData().getPath());
            }
        }
    };
    
    /**
     * 监控/assign
     */
    PathChildrenCacheListener assignCacheListener = new PathChildrenCacheListener() {
    	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
            logger.info("Event path: " + event.getData().getPath());
            
            if(event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
            	try {
            		String worker=event.getData().getPath().substring(event.getData().getPath().lastIndexOf('/') + 1);
            		PathChildrenCache assignTaskCache=new PathChildrenCache(client, event.getData().getPath(), true); 
                	assignTaskCache.getListenable().addListener(assignTaskCacheListener);
					assignTaskCache.start();
					assignTaskMap.put(worker, assignTaskCache);
				} catch (Exception e) {
					logger.warn("assignCacheListener error: "+e.getMessage());
				}
            }
            else if(event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
            	try {
            		String worker=event.getData().getPath().substring(event.getData().getPath().lastIndexOf('/') + 1);
                	PathChildrenCache assignTaskCache=assignTaskMap.get(worker);
                	assignTaskMap.remove(worker);
					assignTaskCache.close();
				} catch (IOException e) {
					logger.warn("assignCacheListener error: "+e.getMessage());
				}
            }
        };
    };
    
    /**
     * 监控/assign/worker
     */
    PathChildrenCacheListener assignTaskCacheListener = new PathChildrenCacheListener() {
    	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
            logger.info("Event path: " + event.getData().getPath());
            
            if(event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
            	//任务成功分配后，删除task
                logger.info("Task assigned correctly: " + event.getData().getPath());
                try {
					deleteTask(event.getData().getPath().substring(event.getData().getPath().lastIndexOf('-') + 1));
				} catch (Exception e) {
					logger.warn("deleteTask failed: "+e.getMessage());
				}
            }
            else if(event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
            	//删除任务分配
                logger.info("Assign correctly deleted: " + event.getData().getPath());
            }
        };
    };
    
    public static void main (String[] args) {
        try{
            MasterCuratorSelector master = new MasterCuratorSelector("localhost:2181", new ExponentialBackoffRetry(1000, 5));
            master.startZK();
            
            master.runForMaster();
            master.awaitLeadership();
            
            master.initNode();
            
            logger.info(">>>>>>Master started, press enter to exit");
    		System.in.read();
    		logger.info(">>>>>>Master exited");
    		
            master.close();
        } catch (Exception e) {
            logger.error("Exception while running curator master.", e);
        }
    }
}
