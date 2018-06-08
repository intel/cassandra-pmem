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

import lib.util.persistent.ObjectPointer;
import lib.util.persistent.PersistentSkipListMap;
import lib.util.persistent.types.ObjectType;

/**
 * Represents the sorted group of column families (rows)
 * in a given partition. The sorting is done based on the
 * clustering key.
 */
public final class PersistentColumnFamilySortedMap extends PersistentSkipListMap
{
    private static final ObjectType<PersistentColumnFamilySortedMap> TYPE = ObjectType.extendClassWith(PersistentColumnFamilySortedMap.class,
                                                                                                       PersistentSkipListMap.class);

    // constructor
    public PersistentColumnFamilySortedMap()
    {
        super(TYPE);
    }

    // reconstructor
    @SuppressWarnings("unused")
    private PersistentColumnFamilySortedMap(ObjectPointer<PersistentColumnFamilySortedMap> pointer)
    {
        super(pointer);
    }
}
