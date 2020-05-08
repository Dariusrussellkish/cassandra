package org.apache.cassandra.timetamp;

import java.nio.ByteBuffer;
import java.io.Serializable;
import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampTag implements Serializable{
    private long timestamp;
    private String writerId ;
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);

    public TimestampTag() {
        this.timestamp = FBUtilities.timestampMicros();
        this.writerId = FBUtilities.getLocalAddressAndPort().toString(false);
    }

    public TimestampTag(long time) {
        this.timestamp = time;
        this.writerId = FBUtilities.getLocalAddressAndPort().toString(false);
    }

    private TimestampTag(String tagString) {
        String[] tagArray = tagString.split(";");
        this.timestamp = Long.parseLong(tagArray[0]);
        this.writerId = tagArray[1];
    }


    public long getTime(){
        return timestamp;
    }

    public String getWriterId() {
        return writerId;
    }

    public TimestampTag updateTag(){
        this.timestamp = FBUtilities.timestampMicros();
        return this;
    }

    public static String serialize(TimestampTag tag) {
        return tag.toString();
    }

    public static TimestampTag deserialize(ByteBuffer buf) {
        String tagString = "";
        try {
            tagString = ByteBufferUtil.string(buf);
        } catch (CharacterCodingException e){
            logger.warn("err casting tag {}",e);
            return new TimestampTag();
        }

        return new TimestampTag(tagString);
    }

    public boolean isLarger(TimestampTag other){
        if(this.timestamp != other.getTime()){
            return this.timestamp - other.getTime() > 0;
        } else {
            return this.writerId.compareTo(other.getWriterId()) > 0;
        }
    }

    public String toString() {
        return timestamp + ";" + writerId;
    }

    public static class TimestampColumns {

        public static final String TAG  = "tag";
        public static final String VAL =  "field0";
    }
}
