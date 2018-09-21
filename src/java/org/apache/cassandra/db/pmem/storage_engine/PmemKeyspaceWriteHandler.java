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

package org.apache.cassandra.db.pmem.storage_engine;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.KeyspaceWriteHandler;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.pmem.storage_engine.PmemWriteContext;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class PmemKeyspaceWriteHandler implements KeyspaceWriteHandler
{
    private final Keyspace keyspace;

    public PmemKeyspaceWriteHandler(Keyspace keyspace)
    {
        this.keyspace = keyspace;
    }

    @Override
    public WriteContext beginWrite(Mutation mutation, boolean makeDurable) throws RequestExecutionException
    {
        return createWriteContext();
    }

    private WriteContext createWriteContext()
    {
        OpOrder.Group group = null;
        try
        {
            group = Keyspace.writeOrder.start();
            return new PmemWriteContext(group);
        }
        catch (Throwable t)
        {
            if (group != null)
            {
                group.close();
            }
            throw t;
        }
    }

    @Override
    public WriteContext createContextForIndexing()
    {
        return createWriteContext();
    }

    @Override
    public WriteContext createContextForRead()
    {
        return createWriteContext();
    }
}
