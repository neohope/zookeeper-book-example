package com.neohope.zk.demo.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Children Cache
 */
public class ChildrenCache {

    protected List<String> children;
    
    public ChildrenCache() {
        this.children = null;        
    }
    
    public ChildrenCache(List<String> children) {
        this.children = children;        
    }
        
    public List<String> getList() {
        return children;
    }
        
    /**
     * 返回：
     * 旧数据+新数据中有但旧数据中没有的
     */
    public List<String> setAndGetBothOldAndNew( List<String> newChildren) {
        ArrayList<String> diff = new ArrayList<String>(newChildren);
        
        for(String s: newChildren) {
            if(!children.contains( s )) {
                if(diff == null) {
                    diff = new ArrayList<String>();
                }
                diff.add(s);
            }
        }
        this.children = newChildren;
            
        return diff;
    }
        
    /**
     * 返回：
     * 旧数据中有，但新数据中没有的
     */
    public List<String> setAndGetOnlyInOld(List<String> newChildren) {
        List<String> diff = null;
        
        if(children != null) {
            for(String s: children) {
                if(!newChildren.contains( s )) {
                    if(diff == null) {
                        diff = new ArrayList<String>();
                    }
                    diff.add(s);
                }
            }
        }
        this.children = newChildren;
        
        return diff;
    }
}
