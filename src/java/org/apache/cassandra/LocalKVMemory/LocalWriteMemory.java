package org.apache.cassandra.LocalKVMemory;

import org.apache.cassandra.db.IMutation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Singleton Monotonic Local Memory Model for BSR.
 * Utilizes thread-safe singleton paradigm to ensure only one instance of this class
 * exists per Cassandra instance. Utilizes ConcurrentHashMap to allow for thread-safe access.
 * Puts into the LocalWriteMemory map are monotonic based on LogicalTimeStamp.
 *
 * @author Darius Russsell Kish
 */
public class LocalWriteMemory {
    // private instance, so that it can be
    // accessed by only by getInstance() method
    volatile private static LocalWriteMemory instance;
    private final Map<String, TagWritePair> localMemory;

    private LocalWriteMemory() {
        this.localMemory = new ConcurrentHashMap<>();
    }

    /**
     * Returns or instantiates the singleton class instance
     *
     * @return LocalWriteMemory instance
     */
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

    /**
     * Monotonic put into the LocalWriteMemory map based on
     * LogicalTimeStamp compareTo.
     *
     * @param key the Cassandra db associated key
     * @param tv the TagWritePair to put into LocalWriteMemory
     * @see org.apache.cassandra.LocalKVMemory.LogicalTimestamp#compareTo(LogicalTimestamp)
     */
    synchronized public void put(String key, TagWritePair tv) {
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

    /**
     * Monotonic put into the LocalWriteMemory map based on
     * LogicalTimeStamp compareTo.
     *
     * @param key the Cassandra db associated key
     * @param ts the LogicalTimestamp to use for the following read result
     * @param v the UnfilteredPartitionIterator read result associated with the LogicalTimestamp
     * @see org.apache.cassandra.LocalKVMemory.LogicalTimestamp#compareTo(LogicalTimestamp)
     */
    synchronized public void put(String key, LogicalTimestamp ts, IMutation v) {
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

    public static class TagWritePair implements Comparable<TagWritePair> {

        private final LogicalTimestamp tag;
        private final IMutation value;

        public TagWritePair(LogicalTimestamp tag, IMutation value) {
            this.tag = tag;
            this.value = value;
        }

        public LogicalTimestamp getTag() {
            return tag;
        }

        public IMutation getValue() {
            return value;
        }

        public int compareTo(TagWritePair other) {
            return this.tag.compareTo(other.getTag());
        }
    }
}
