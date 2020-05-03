package org.apache.cassandra.LocalKVMemory;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.LogicalTimestamp;

public class TagWritePair implements Comparable<TagWritePair> {

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
