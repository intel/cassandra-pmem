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

import java.util.UUID;

import org.apache.cassandra.schema.TableMetadata;

public interface MTablesManager
{
    /**
     * Check if {@link org.apache.cassandra.memory.persistent.PMTable}
     * exists for a given unique identifier
     *
     * @param tableId unique identifier (UUID) for a table
     * @return true if the table exists. False otherwise.
     */
    public boolean doesMTableExist(UUID tableId);

    /**
     * Create {@link org.apache.cassandra.memory.persistent.PMTable}
     *
     * @param tableId                  unique table identifier
     * @param isClusteringKeyAvailable true if the table has the clustering key(s). False otherwise.
     * @param tableMetadata            table metadata generated in in-memory storage format
     * @param tempMetadata             original table metadata
     * @return True if the table got created. False otherwise.
     */
    public boolean createMTableIfAbsent(UUID tableId,
                                        boolean isClusteringKeyAvailable,
                                        MTableMetadata tableMetadata,
                                        TableMetadata tempMetadata);

    /**
     * Get {@link org.apache.cassandra.memory.persistent.PMTable}
     * for a given identifier
     *
     * @param tableId table unique identifier
     * @return in-memory table instance
     */
    public MTable getMTable(UUID tableId);
}
