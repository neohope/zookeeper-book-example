package com.neohope.zk.demo.utils;

/**
 * ZooKeeper task
 */
public class ZkTask {
	public String path; 
	public String task;
	public byte[] data;
    
    public ZkTask(String path, String task, byte[] data) {
        this.path = path;
        this.task = task;
        this.data = data;
    }
}
