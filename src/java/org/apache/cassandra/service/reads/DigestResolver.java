/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service.reads;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.Collections;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.LogicalTimestampColumns;
import org.apache.cassandra.service.LogicalTimestamp;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DigestResolver extends ResponseResolver
{
    private volatile ReadResponse dataResponse;

    public DigestResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, ReadRepair readRepair, int maxResponseCount)
    {
        super(keyspace, command, consistency, readRepair, maxResponseCount);
        Preconditions.checkArgument(command instanceof SinglePartitionReadCommand,
                                    "DigestResolver can only be used with SinglePartitionReadCommand commands");
    }

    @Override
    public void preprocess(MessageIn<ReadResponse> message)
    {
        super.preprocess(message);
        if (dataResponse == null && !message.payload.isDigestResponse())
            dataResponse = message.payload;
    }

    // this is the original method, NoopReadRepair has a call to this method
    // simply change the method signature to ReadResponse getData() will raise an compiler error
    public PartitionIterator getData()
    {
        assert isDataPresent();
        return UnfilteredPartitionIterators.filter(dataResponse.makeIterator(command), command.nowInSec());
    }

    // this is a new method for AbstractReadExecutor, which may want to use ReadResponse more than once
    public ReadResponse getReadResponse()
    {
        assert isDataPresent();
        return dataResponse;
    }

    public boolean responsesMatch()
    {
        long start = System.nanoTime();

        // validate digests against each other; return false immediately on mismatch.
        ByteBuffer digest = null;
        for (MessageIn<ReadResponse> message : responses)
        {
            ReadResponse response = message.payload;

            ByteBuffer newDigest = response.digest(command);
            if (digest == null)
                digest = newDigest;
            else if (!digest.equals(newDigest))
                // rely on the fact that only single partition queries use digests
                return false;
        }

        if (logger.isTraceEnabled())
            logger.trace("responsesMatch: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        return true;
    }

    private class TagResponsePair implements Comparable<TagResponsePair>
    {
        private LogicalTimestamp tag;
        private ReadResponse response;

        public TagResponsePair(LogicalTimestamp tag, ReadResponse response)
        {
            this.tag = tag;
            this.response = response;
        }

        public int compareTo(TagResponsePair other)
        {
            return this.tag.compareTo(other.tag);
        }

        public LogicalTimestamp getTag() {
            return this.tag;
        }

        public ReadResponse getResponse() {
            return this.response;
        }
    }

    public ReadResponse getBSRResponse()
    {
        // check all data responses,
        // extract the one with max z value
        LogicalTimestamp maxTag = new LogicalTimestamp();
        ReadResponse maxResponse = null;

        // TODO: find a way to get this from the replication factor
        // Store results in a maxheap so we can get the (f+1)th highest tag
        int replicationFactor = 20;
        PriorityBlockingQueue<TagResponsePair> sortedTags = new PriorityBlockingQueue<>(replicationFactor, Collections.reverseOrder());

        ColumnIdentifier zIdentifier = new ColumnIdentifier(LogicalTimestampColumns.TAG, true);
        for (MessageIn<ReadResponse> message : responses)
        {
            ReadResponse curResponse = message.payload;

            // check if the response is indeed a data response
            // we shouldn't get a digest response here
            assert curResponse.isDigestResponse() == false;

            // get the partition iterator corresponding to the
            // current data response
            PartitionIterator pi = UnfilteredPartitionIterators.filter(curResponse.makeIterator(command), command.nowInSec());
            // get the z value column
            while(pi.hasNext())
            {
                // zValueReadResult.next() returns a RowIterator
                RowIterator ri = pi.next();
                while(ri.hasNext())
                {
                    ColumnMetadata tagMetaData = ri.metadata().getColumn(ByteBufferUtil.bytes(LogicalTimestampColumns.TAG));
                    Row r = ri.next();

                    // todo: the entire row is read for the sake of development
                    // future improvement could be made

                    LogicalTimestamp curTag = new LogicalTimestamp();
                    Cell tagCell = r.getCell(tagMetaData);
                    LogicalTimestamp readingTag = LogicalTimestamp.deserialize(tagCell.value());
                    if(tagCell!=null && readingTag!=null){
                        curTag = readingTag;
                    }

                    // add tag to max heap
                    sortedTags.add(new TagResponsePair(curTag, curResponse));
                }
            }
        }

        // remove max values for f+1 iterations
        for (int i = 0; i < ConsistencyLevel.ByzantineFaultTolerance + 1; i++) {
            TagResponsePair nthMax = sortedTags.poll();
            if (nthMax != null)
            {
               maxResponse = nthMax.getResponse();
            }
        }

        return maxResponse;
    }

    public boolean isDataPresent()
    {
        return dataResponse != null;
    }
}
