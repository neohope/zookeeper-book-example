package com.neohope.zk.demo.utils;

import java.util.List;

/**
 * 恢复任务，回调接口
 */
public interface IRecoveryCallback {
    final static int OK = 0;
    final static int FAILED = -1;
    
    public void recoveryComplete(int rc, List<String> tasks);
}
