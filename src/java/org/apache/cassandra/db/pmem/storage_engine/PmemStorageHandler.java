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

import org.apache.cassandra.db.pmem.TablesManager;
import org.jctools.maps.NonBlockingHashMap;

public class PmemStorageHandler
{
    public TablesManager tablesManager;
    private static PmemStorageHandler pmemStorageHandler;
    private static NonBlockingHashMap<String, TablesManager> keyspaceMap;

    private PmemStorageHandler()
    {

    }

    public static PmemStorageHandler getInstance()
    {
        if(pmemStorageHandler == null)
        {
            pmemStorageHandler = new PmemStorageHandler();
            keyspaceMap = new NonBlockingHashMap<>();
        }
        return pmemStorageHandler;
    }

    public void setTablesManager(String keyspaceName,TablesManager tablesManager)
    {
        this.tablesManager = tablesManager;
        keyspaceMap.putIfAbsent(keyspaceName,tablesManager);
    }

    public TablesManager getTablesManager(String keyspaceName)
    {
        return keyspaceMap.get(keyspaceName);
    }
}
