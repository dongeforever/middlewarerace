package com.alibaba.middleware.race.store;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by liuzhendong on 16/7/19.
 * 基于LinkedHashMap实现一个简单的lrucache
 */
public class LruCache<K,V> extends LinkedHashMap<K,V>{

    private int maxCapacity;
    private String name;
    public LruCache(int initialCapacity,int maxCapacity,String name){
        super(initialCapacity, 0.75f,true);
        this.maxCapacity = maxCapacity;
        this.name = name;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
        if(super.size() > maxCapacity) return true;
        return false;
    }


    @Override
    public  V get(Object key){
        synchronized (this){
            return super.get(key);
        }
    }

    @Override
    public V put(K key, V value){
        synchronized (this){
            return super.put(key,value);
        }
    }
}
