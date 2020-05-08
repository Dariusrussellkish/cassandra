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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.timetamp.TimestampTag;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutationVerbHandler implements IVerbHandler<Mutation>
{
    private static final Logger logger = LoggerFactory.getLogger(MutationVerbHandler.class);

    private void reply(int id, InetAddressAndPort replyTo)
    {
        Tracing.trace("Enqueuing response to {}", replyTo);
        MessagingService.instance().sendReply(WriteResponse.createMessage(), id, replyTo);
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    public void doVerb(MessageIn<Mutation> message, int id)  throws IOException
    {
        // Check if there were any forwarding headers in this message
        InetAddressAndPort from = (InetAddressAndPort)message.parameters.get(ParameterType.FORWARD_FROM);
        InetAddressAndPort replyTo;
        if (from == null) {
            replyTo = message.from;
            ForwardToContainer forwardTo = (ForwardToContainer)message.parameters.get(ParameterType.FORWARD_TO);
            if (forwardTo != null)
                forwardToLocalNodes(message.payload, message.verb, forwardTo, message.from);
        } else {
            replyTo = from;
        }

        // comparing the tag and the one in mutation, act accordingly
        if (canUpdate(message.payload)) {
            try {
                message.payload.applyFuture().thenAccept(o -> reply(id, replyTo));
            } catch (WriteTimeoutException wto) {
                failed();
            }
        } else {
            reply(id,replyTo);
        }
    }

    public static boolean canUpdate(IMutation mutation){
        // first we have to create a read request out of the current mutation
        SinglePartitionReadCommand localRead = SinglePartitionReadCommand.tagRead(
                mutation.getPartitionUpdates().iterator().next().metadata(),
                FBUtilities.nowInSeconds(),
                mutation.key());

        // execute the read request locally to obtain the tag of the key
        // and extract tag information from the local read
        TimestampTag tagLocal = null;
        try (ReadExecutionController executionController = localRead.executionController();
             UnfilteredPartitionIterator iterator = localRead.executeLocally(executionController)) {
            // first we have to transform it into a PartitionIterator
            PartitionIterator pi = UnfilteredPartitionIterators.filter(iterator, localRead.nowInSec());
            while(pi.hasNext()) {
                RowIterator ri = pi.next();
                while(ri.hasNext()) {
                    Row r = ri.next();
                    ColumnMetadata colMeta = ri.metadata().getColumn(ByteBufferUtil.bytes(TimestampTag.TimestampColumns.TAG));
                    Cell c = r.getCell(colMeta);
                    if (c == null) {
                        logger.error(r.toString());
                    }else {
                        tagLocal = TimestampTag.deserialize(c.value());
                    }
                }
            }
        }

        // extract the tag information from the mutation
        TimestampTag tagRemote = new TimestampTag();
        Row data = mutation.getPartitionUpdates().iterator().next().getRow(Clustering.EMPTY);
        ColumnIdentifier ci = new ColumnIdentifier(TimestampTag.TimestampColumns.TAG,true);

        for (Cell c : data.cells()) {
            if(c.column().name.equals(ci)) {
                tagRemote = TimestampTag.deserialize(c.value());
                break;
            }
        }
        if (tagLocal == null) return true;
        return tagRemote.isLarger(tagLocal);
    }

    private static void forwardToLocalNodes(Mutation mutation, MessagingService.Verb verb, ForwardToContainer forwardTo, InetAddressAndPort from) throws IOException
    {
        // tell the recipients who to send their ack to
        MessageOut<Mutation> message = new MessageOut<>(verb, mutation, Mutation.serializer).withParameter(ParameterType.FORWARD_FROM, from);
        Iterator<InetAddressAndPort> iterator = forwardTo.targets.iterator();
        // Send a message to each of the addresses on our Forward List
        for (int i = 0; i < forwardTo.targets.size(); i++)
        {
            InetAddressAndPort address = iterator.next();
            Tracing.trace("Enqueuing forwarded write to {}", address);
            MessagingService.instance().sendOneWay(message, forwardTo.messageIds[i], address);
        }
    }
}
