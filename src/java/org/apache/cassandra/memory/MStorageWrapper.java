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

package org.apache.cassandra.memory;

import org.apache.cassandra.memory.persistent.PMStorageWrapper;

public interface MStorageWrapper
{
    /**
     * Generate/return the object that holds the information about
     * keyspaces and corresponding set of column families.
     *
     * @return {@link PMStorageWrapper} instance
     * based on the configuration
     */
    public static MStorageWrapper getInstance()
    {
        return PMStorageWrapper.getInstance();
    }

    /**
     * Insert an entry for a given keyspace name, if doesn't exist already
     *
     * @param keyspace
     * @return true, if keyspace was created successfully. False otherwise.
     */
    public boolean createKeyspaceIfAbsent(String keyspace);

    /**
     * Check if a keyspace exists
     *
     * @param keyspace
     * @return true if the giveb keyspace exist. False, otherwise
     */
    public boolean doesKeyspaceExist(String keyspace);

    /**
     * Get {@link org.apache.cassandra.memory.persistent.PMTablesManager}
     * for the given keyspace
     *
     * @param keyspace
     * @return return MTablesManager depending on what is enabled: persistent or volatile
     */
    public MTablesManager getMTablesManager(String keyspace);
}
