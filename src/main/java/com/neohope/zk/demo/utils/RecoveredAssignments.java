package com.neohope.zk.demo.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 遍历节点，对于已挂掉的worker的任务分配
 * 重新建立任务，删除任务分配和任务状态
 */
public class RecoveredAssignments {
    private static final Logger logger = LoggerFactory.getLogger(RecoveredAssignments.class);
    
    //待分配的任务列表，包括重建任务（worker挂掉的）
    List<String> tasks;
    
    //已分配任务状态列表
    List<String> status;

    //有效的分配（worker活着）
    List<String> assignments;

    //活着的worker
    List<String> activeWorkers;

    //已有任务分配的workers，包括挂掉的
    List<String> assignedWorkers;
    
    IRecoveryCallback cb;
    ZooKeeper zk;

    public RecoveredAssignments(ZooKeeper zk){
        this.zk = zk;
        this.assignments = new ArrayList<String>();
    }
    
    /**
     * 开始恢复
     * 获取各个节点
     */
    public void recover(IRecoveryCallback recoveryCallback){
        cb = recoveryCallback;
        getTasks();
    }
    
	/**
	 * 获取tasks节点，然后获取assign，然后获取workers，然后获取assign开始业务处理
	 */
    private void getTasks(){
        zk.getChildren("/tasks", false, tasksCallback, null);
    }
    
    ChildrenCallback tasksCallback = new ChildrenCallback(){
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                logger.info("Getting tasks for recovery");
                tasks = children;
                getAssignedWorkers();
                break;
            default:
                logger.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
                cb.recoveryComplete(IRecoveryCallback.FAILED, null);
            }
        }
    };
    
	/**
	 * 获取assign子节点，然后获取workers，然后获取assign开始业务处理
	 */
    private void getAssignedWorkers(){
        zk.getChildren("/assign", false, assignedWorkersCallback, null);
    }
    
    ChildrenCallback assignedWorkersCallback = new ChildrenCallback(){
        public void processResult(int rc, String path, Object ctx, List<String> children){    
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getAssignedWorkers();
                break;
            case OK:  
                assignedWorkers = children;
                getWorkers(children);
                break;
            default:
                logger.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
                cb.recoveryComplete(IRecoveryCallback.FAILED, null);
            }
        }
    };
        
	/**
	 * 获取workers子节点，然后获取assign开始业务处理
	 */
    private void getWorkers(Object ctx){
        zk.getChildren("/workers", false, workersCallback, ctx);
    }
    
    ChildrenCallback workersCallback = new ChildrenCallback(){
        public void processResult(int rc, String path, Object ctx, List<String> children){    
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getWorkers(ctx);
                break;
            case OK:
                logger.info("Getting worker assignments for recovery: " + children.size());
                if(children.size()==0) {
                    logger.warn( "Empty list of workers, possibly just starting" );
                }
                
                activeWorkers = children;
                if(assignedWorkers.size()==0){
                	cb.recoveryComplete(IRecoveryCallback.OK, tasks);    
                }
                else{
	                for(String s : assignedWorkers){
	                    getWorkerAssignments("/assign/" + s);
	                }
                }
                
                break;
            default:
                logger.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
                cb.recoveryComplete(IRecoveryCallback.FAILED, null);
            }
        }
    };
    
	/**
	 * 获取每个worker的任务分配
	 * 对于已经挂掉的worker，重新提交任务，删除任务分配，删除任务状态
	 */
    private void getWorkerAssignments(String s) {
        zk.getChildren(s, false, workerAssignmentsCallback, null);
    }
    
    ChildrenCallback workerAssignmentsCallback = new ChildrenCallback(){
        public void processResult(int rc, String path, Object ctx, List<String> children) {    
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getWorkerAssignments(path);
                break;
            case OK:
                String worker = path.replace("/assign/", "");
                if(activeWorkers.contains(worker)) {
                    assignments.addAll(children);
                } else {
                	if(children.size()==0)deleteAssignment(path);
                	else {
                		for( String task : children ) {
	                        if(!tasks.contains( task )) {
	                            tasks.add( task );
	                            getDataReassign( path, task );
	                        } else {
	                            deleteAssignment(path + "/" + task);
	                        }
                		}
                    }
                }
                   
                assignedWorkers.remove(worker);
                if(assignedWorkers.size() == 0){
                    logger.info("Getting statuses for recovery");
                    getStatuses();
                 }
                
                break;
            case NONODE:
                logger.info( "No such znode exists: " + path );
                
                break;
            default:
                logger.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
                cb.recoveryComplete(IRecoveryCallback.FAILED, null);
            }
        }
    };
    
    /**
     * 获取任务分配节点的数据，重新提交任务，删除任务分配，删除任务状态
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
                logger.error("Something went wrong when getting data ",
                        KeeperException.create(Code.get(rc)));
            }
        }
    };
    
    /**
     * 重新提交任务，删除任务分配，删除任务状态
     */
    void recreateTask(ZkTask ctx) {
        zk.create("/tasks/" + ctx.task, ctx.data, Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT, recreateTaskCallback, ctx);
    }
    
    StringCallback recreateTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                recreateTask((ZkTask) ctx);
                break;
            case OK:
                deleteAssignment(((ZkTask)ctx).path+"/"+((ZkTask)ctx).task);
                break;
            case NODEEXISTS:
                logger.warn("Node shouldn't exist: " + path);
                break;
            default:
                logger.error("Something wwnt wrong when recreating task", 
                        KeeperException.create(Code.get(rc)));
            }
        }
    };
    
    /**
     * 删除任务分配，删除任务状态
     */
    void deleteAssignment(String path){
        zk.delete(path, -1, assignDeletionCallback, null);
    }
    
    VoidCallback assignDeletionCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object rtx){
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteAssignment(path);
                break;
            case OK:
                logger.info("Task correctly deleted: " + path);
                break;
            default:
                logger.error("Failed to delete assign data" + 
                        KeeperException.create(Code.get(rc), path));
            } 
        }
    };
    
    /**
     * 获取任务状态status
     */
    void getStatuses(){
        zk.getChildren("/status", false, statusCallback, null); 
    }
    
    ChildrenCallback statusCallback = new ChildrenCallback(){
        public void processResult(int rc, 
                String path, 
                Object ctx, 
                List<String> children){    
            switch (Code.get(rc)) { 
            case CONNECTIONLOSS:
                getStatuses();
                break;
            case OK:
                logger.info("Processing assignments for recovery");
                status = children;
                processAssignments();
                break;
            default:
                logger.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
                cb.recoveryComplete(IRecoveryCallback.FAILED, null);
            }
        }
    };
    
    /**
     * 删除任务分配，删除任务状态
     */
    private void processAssignments(){
        logger.info("Size of tasks: " + tasks.size());
        
        //删除已分配任务
        for(String s: assignments){
            logger.info("Assignment: " + s);
            deleteTask("/tasks/" + s);
            tasks.remove(s);
        }
        
        //删除已完成的任务
        logger.info("Size of tasks after assignment filtering: " + tasks.size());
        for(String s: status){
            logger.info( "Checking task: {} ", s );
            deleteTask("/tasks/" + s);
            tasks.remove(s);
        }
        logger.info("Size of tasks after status filtering: " + tasks.size());
        
        // 处理成功，回调返回
        cb.recoveryComplete(IRecoveryCallback.OK, tasks);     
    }
    
    /**
     * 删除任务
     */
    void deleteTask(String path){
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
                logger.error("Failed to delete task data" + KeeperException.create(Code.get(rc), path));
            } 
        }
    };
}

