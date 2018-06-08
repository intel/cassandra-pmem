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

import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

public interface MCFFilter
{
    public Unfiltered getUnfilteredRowIterator(Map.Entry<MClusteringKey, MRow> persistentRowEntry, TableMetadata metadata);

    public Unfiltered getUnfilteredRowSingle(MRow mRow, Clustering clustering, TableMetadata metadata);

    public UnfilteredRowIterator getRowIterator(ClusteringIndexNamesFilter filter, Object persistentVal, DecoratedKey dKey);

    public UnfilteredRowIterator getSliceIterator(ClusteringIndexFilter filter, Iterator<Map.Entry<MClusteringKey, MRow>> iter, TableMetadata metadata, MTableMetadata mTableMetadata, DecoratedKey decoratedKey);
}
