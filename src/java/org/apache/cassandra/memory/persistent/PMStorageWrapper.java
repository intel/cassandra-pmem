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

package org.apache.cassandra.memory.persistent;

import lib.util.persistent.ObjectDirectory;
import lib.util.persistent.PersistentSIHashMap;
import lib.util.persistent.PersistentString;
import org.apache.cassandra.memory.MStorageWrapper;
import org.apache.cassandra.memory.MTablesManager;

// It provides the main global interface to interact with PM storage framework.
public class PMStorageWrapper implements MStorageWrapper
{
    // visible for testing
    public static PersistentSIHashMap<PersistentString, PMTablesManager> ksManager = null;
    private static PMStorageWrapper pmStorageWrapper = null;
    public static final String ROOT_CASSANDRA_DB = "ROOT_CASSANDRA_DB";

    static
    {
        ksManager = ObjectDirectory.get(ROOT_CASSANDRA_DB, PersistentSIHashMap.class);
        if (ksManager == null)
            ObjectDirectory.put(ROOT_CASSANDRA_DB, ksManager = new PersistentSIHashMap<>());
    }

    // Private constructor allow only one static instance of keyspace manager
    private PMStorageWrapper()
    {
    }

    public static synchronized PMStorageWrapper getInstance()
    {
        if (pmStorageWrapper == null)
            pmStorageWrapper = new PMStorageWrapper();
        return pmStorageWrapper;
    }

    // Create keyspace only if it is absent.
    // We don't want to replace the existing table manager instance as value
    @Override
    public boolean createKeyspaceIfAbsent(String keyspace)
    {
        PMTablesManager pmTablesManager = ksManager.putIfAbsent(PersistentString.make(keyspace),
                                                                new PMTablesManager());
        if (pmTablesManager == null)
            return true;
        else
            return false; // keyspace exists already
    }

    // Exposing this API for now just in case it is used in future
    @Override
    public boolean doesKeyspaceExist(String keyspace)
    {
        if (ksManager.containsKey(PersistentString.make(keyspace)))
            return true;
        else
            return false;
    }

    // Return the tables manager instance for a given keyspace
    @Override
    public MTablesManager getMTablesManager(String keyspace)
    {
        return ksManager.get(keyspace, PersistentString.class);
    }
}
