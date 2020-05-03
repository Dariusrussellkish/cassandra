package org.apache.cassandra.LocalKVMemory;

import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.service.LogicalTimestamp;

public class TagReadPair implements Comparable<TagReadPair> {

    private final LogicalTimestamp tag;
    private final UnfilteredPartitionIterator value;

    public TagReadPair(LogicalTimestamp tag, UnfilteredPartitionIterator value) {
        this.tag = tag;
        this.value = value;
    }

    public LogicalTimestamp getTag() {
        return tag;
    }

    public UnfilteredPartitionIterator getValue() {
        return value;
    }

    public int compareTo(TagReadPair other) {
        return this.tag.compareTo(other.getTag());
    }
}
