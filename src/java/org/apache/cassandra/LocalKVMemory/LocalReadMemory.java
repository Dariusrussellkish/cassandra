package org.apache.cassandra.LocalKVMemory;

import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.service.LogicalTimestamp;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocalReadMemory {
    // private instance, so that it can be
    // accessed by only by getInstance() method
    private static LocalReadMemory instance;
    private final Map<String, TagReadPair> localMemory;

    private LocalReadMemory() {
        this.localMemory = new ConcurrentHashMap<>();
    }

    public static LocalReadMemory getInstance() {
        if (instance == null) {
            synchronized (LocalReadMemory.class) {
                if(instance==null) {
                    instance = new LocalReadMemory();
                }
            }
        }
        return instance;
    }

    public void put(String key, TagReadPair tv) {
        TagReadPair localtv = get(key);
        if (localtv != null) {
            if (localtv.compareTo(tv) < 0) {
                localMemory.put(key, tv);
            }
            // nop
            else return;
        } else {
            localMemory.put(key, tv);
        }
    }

    public void put(String key, LogicalTimestamp ts, UnfilteredPartitionIterator v) {
        TagReadPair localtv = get(key);
        if (localtv != null) {
            if (localtv.getTag().compareTo(ts) < 0) {
                localMemory.put(key, new TagReadPair(ts, v));
            }
            // nop
            else return;
        } else {
            localMemory.put(key, new TagReadPair(ts, v));
        }
    }

    public TagReadPair get(String key) {
        return localMemory.getOrDefault(key, null);
    }
}
