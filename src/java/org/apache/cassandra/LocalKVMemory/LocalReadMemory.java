package org.apache.cassandra.LocalKVMemory;

import org.apache.cassandra.db.ReadResponse;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Singleton Monotonic Local Memory Model for BSR.
 * Utilizes thread-safe singleton paradigm to ensure only one instance of this class
 * exists per Cassandra instance. Utilizes ConcurrentHashMap to allow for thread-safe access.
 * Puts into the LocalReadMemory map are monotonic based on LogicalTimeStamp.
 *
 * @author Darius Russsell Kish
 */
public class LocalReadMemory {
    // private instance, so that it can be
    // accessed by only by getInstance() method
    volatile private static LocalReadMemory instance;
    private final Map<String, TagReadPair> localMemory;

    private LocalReadMemory() {
        this.localMemory = new ConcurrentHashMap<>();
    }

    /**
     * Returns or instantiates the singleton class instance
     *
     * @return LocalReadMemory instance
     */
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

    /**
     * Monotonic put into the LocalReadMemory map based on
     * LogicalTimeStamp compareTo.
     *
     * @param key the Cassandra db associated key
     * @param tv the TagReadPair to put into LocalReadMemory
     * @see org.apache.cassandra.LocalKVMemory.LogicalTimestamp#compareTo(LogicalTimestamp)
     */
    synchronized public void put(String key, TagReadPair tv) {
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

    /**
     * Monotonic put into the LocalReadMemory map based on
     * LogicalTimeStamp compareTo.
     *
     * @param key the Cassandra db associated key
     * @param ts the LogicalTimestamp to use for the following read result
     * @param v the UnfilteredPartitionIterator read result associated with the LogicalTimestamp
     * @see org.apache.cassandra.LocalKVMemory.LogicalTimestamp#compareTo(LogicalTimestamp)
     */
    synchronized public void put(String key, LogicalTimestamp ts, ReadResponse v) {
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

    public static class TagReadPair implements Comparable<TagReadPair> {

        private final LogicalTimestamp tag;
        private final ReadResponse value;

        public TagReadPair(LogicalTimestamp tag, ReadResponse value) {
            this.tag = tag;
            this.value = value;
        }

        public LogicalTimestamp getTag() {
            return tag;
        }

        public ReadResponse getValue() {
            return value;
        }

        public int compareTo(TagReadPair other) {
            return this.tag.compareTo(other.getTag());
        }
    }
}
