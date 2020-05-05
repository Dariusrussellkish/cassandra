package org.apache.cassandra.LocalKVMemory;

import java.nio.ByteBuffer;
import java.io.Serializable;
import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogicalTimestamp implements Serializable, Comparable<LogicalTimestamp>
{
    private int logicalTIme;
    private final String writerId ;
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);

    public LogicalTimestamp(){
        this.logicalTIme = -1;
        this.writerId = FBUtilities.getLocalAddressAndPort().toString(false);
    }

    private LogicalTimestamp(String tagString){
        String[] tagArray = tagString.split(";");
        this.logicalTIme = Integer.parseInt(tagArray[0]);
        this.writerId = tagArray[1];
    }


    public int getTime(){
        return logicalTIme;
    }

    public String getWriterId() {
        return writerId;
    }

    public LogicalTimestamp nextTag(){
        this.logicalTIme++;
        return this;
    }

    public static String serialize(LogicalTimestamp tag) {
        return tag.toString();
    }

    public static LogicalTimestamp deserialize(ByteBuffer buf) {
        String tagString;
        try {
            tagString = ByteBufferUtil.string(buf);
        } catch (CharacterCodingException e){
            logger.warn("err casting tag %s", e);
            return new LogicalTimestamp();
        }

        return new LogicalTimestamp(tagString);
    }

    /**
     * @deprecated
     *
     * @param other LogicalTimestamp to compare against
     * @return boolean if LogicalTimestamp is larger
     */
    public boolean isLarger(LogicalTimestamp other){
        if(this.logicalTIme != other.getTime()){
            return this.logicalTIme - other.getTime() > 0;
        } else {
            return this.writerId.compareTo(other.getWriterId()) > 0;
        }
    }

    /**
     *
     * @param other LogicalTimestamp to compare against
     * @return integer of LogicalTimestamp compare
     */
    public int compareTo(LogicalTimestamp other)
    {
        if (this.logicalTIme != other.getTime()) {
            return this.logicalTIme - other.getTime();
        } else {
            return this.writerId.compareTo(other.getWriterId());
        }
    }

    public String toString() {
        return logicalTIme + ";" + writerId;
    }
}
