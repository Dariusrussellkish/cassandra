package org.apache.cassandra.LocalKVMemory;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.service.LogicalTimestamp;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LocalWriteMemory {
    // private instance, so that it can be
    // accessed by only by getInstance() method
    private static LocalWriteMemory instance;
    private final Map<String, TagWritePair> localMemory;

    private LocalWriteMemory() {
        this.localMemory = new ConcurrentHashMap<>();
    }

    public static LocalWriteMemory getInstance() {
        if (instance == null) {
            synchronized (LocalWriteMemory.class) {
                if(instance==null) {
                    instance = new LocalWriteMemory();
                }
            }
        }
        return instance;
    }

    public void put(String key, TagWritePair tv) {
        TagWritePair localtv = get(key);
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

    public void put(String key, LogicalTimestamp ts, IMutation v) {
        TagWritePair localtv = get(key);
        if (localtv != null) {
            if (localtv.getTag().compareTo(ts) < 0) {
                localMemory.put(key, new TagWritePair(ts, v));
            }
            // nop
            else return;
        } else {
            localMemory.put(key, new TagWritePair(ts, v));
        }
    }

    public TagWritePair get(String key) {
        return localMemory.getOrDefault(key, null);
    }
}
