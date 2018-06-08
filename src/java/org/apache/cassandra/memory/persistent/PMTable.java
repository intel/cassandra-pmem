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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import lib.util.persistent.AnyPersistent;
import lib.util.persistent.ObjectPointer;
import lib.util.persistent.PersistentFPTree2;
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.types.FinalBooleanField;
import lib.util.persistent.types.FinalObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.memory.MDecoratedKey;
import org.apache.cassandra.memory.MRow;
import org.apache.cassandra.memory.MTable;
import org.apache.cassandra.memory.MTableMetadata;
import org.apache.cassandra.memory.MTableUnfilteredPartitionIterator;
import org.apache.cassandra.memory.MToken;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.memory.MUtils;


/**
 * This class represents the PMTable for a given Column Family.
 * It maintains the mappings for <Partition Key, Rows> for a given column family.
 */
public final class PMTable extends PersistentImmutableObject implements MTable
{
    private static final FinalObjectField<PersistentFPTree2<PersistentDecoratedKey, AnyPersistent>> PMTABLE = new FinalObjectField<>();
    private static final FinalBooleanField IS_CK_AVAILABLE = new FinalBooleanField();
    private static final FinalObjectField<PersistentTableMetadata> PM_METADATA = new FinalObjectField<>();

    private static final ObjectType<PMTable> TYPE = ObjectType.fromFields(PMTable.class,
                                                                          PMTABLE,
                                                                          IS_CK_AVAILABLE,
                                                                          PM_METADATA);

    public TableMetadata tableMetadata;


    // constructor
    public PMTable(MTableMetadata mTableMetadata, boolean isClusteringKeyAvailable, TableMetadata tableMetadata)
    {
        super(TYPE, (PMTable self) ->
        {
            self.initBooleanField(IS_CK_AVAILABLE, isClusteringKeyAvailable);
            self.initObjectField(PM_METADATA, (PersistentTableMetadata) mTableMetadata);
            self.initObjectField(PMTABLE, new PersistentFPTree2<>());
        });
        this.tableMetadata = tableMetadata;
    }

    // reconstructor
    @SuppressWarnings("unused")
    private PMTable(ObjectPointer<? extends PMTable> pointer)
    {
        super(pointer);
        tableMetadata = MUtils.getTableMetadataFromPM(getTableMetadata());
    }

    public PersistentFPTree2<PersistentDecoratedKey, AnyPersistent> getPMTable()
    {
        return getObjectField(PMTABLE);
    }

    @Override
    public MTableMetadata getTableMetadata()
    {
        return getObjectField(PM_METADATA);
    }

    @Override
    public boolean doesClusteringKeyExist()
    {
        return getBooleanField(IS_CK_AVAILABLE);
    }

    // Insert partition key if doesn't exist already.
    @Override
    public void putPartitionKeyIfAbsent(MDecoratedKey partitionPosition)
    {
        if (getBooleanField(IS_CK_AVAILABLE))
        {
            getPMTable().putIfAbsent((PersistentDecoratedKey) partitionPosition,
                                     new PersistentColumnFamilySortedMap());
        }
    }

    //@Override
    public void putRow(MDecoratedKey key, MRow mRow)
    {

    }

    // The return type can either be PersistentRow or PersistentColumnFamilySortedMap
    // TODO: Should capture the case failure in order to avoid server crash
    @Override
    public <T> T get(MDecoratedKey partitionPosition)
    {
        try
        {
            return (T) getPMTable().get(partitionPosition);
        }
        catch (ClassCastException e)
        {
            e.printStackTrace(); // TODO: log this exception
            return null;
        }
    }

    @Override
    public <T> T get(DecoratedKey partitionPosition)
    {
        try
        {
            return (T) getPMTable().get(partitionPosition, PersistentDecoratedKey.class);
        }
        catch (ClassCastException e)
        {
            e.printStackTrace(); // TODO: log this exception
            return null;
        }
    }

    /* An iterator for PMTable */

    @Override
    public MTableUnfilteredPartitionIterator makePartitionIterator(ColumnFilter columnFilter, DataRange dataRange, TableMetadata metadata)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        boolean startIsMin = keyRange.left.isMinimum();
        boolean stopIsMin = keyRange.right.isMinimum();

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;
        Map<PersistentDecoratedKey, AnyPersistent> subMap;


        if (startIsMin)
        {
            if (stopIsMin)
                subMap = getPMTable();
            else
            {
                DecoratedKey dkey = (DecoratedKey) keyRange.right;
                MToken token = PMToken.getInstance(dkey.getToken().getTokenValue());
                PersistentDecoratedKey pdk = new PersistentDecoratedKey(dkey.getKey().array(), token);
                subMap = getPMTable().headMap(pdk, includeStop);
            }
        }
        else if (stopIsMin)
        {
            DecoratedKey dkey = (DecoratedKey) keyRange.left;
            MToken token = PMToken.getInstance(dkey.getToken().getTokenValue());
            PersistentDecoratedKey pdk = new PersistentDecoratedKey(dkey.getKey().array(), token);
            subMap = getPMTable().tailMap(pdk, includeStart);
        }
        else
        {


            if (keyRange.left instanceof DecoratedKey && keyRange.right instanceof DecoratedKey)
            {
                DecoratedKey dkeyLeft = (DecoratedKey) keyRange.left;
                MToken tokenLeft = PMToken.getInstance(dkeyLeft.getToken().getTokenValue());
                PersistentDecoratedKey pdkLeft = new PersistentDecoratedKey(dkeyLeft.getKey().array(), tokenLeft);

                DecoratedKey dkeyRight = (DecoratedKey) keyRange.right;
                MToken tokenRight = PMToken.getInstance(dkeyRight.getToken().getTokenValue());
                PersistentDecoratedKey pdkRight = new PersistentDecoratedKey(dkeyRight.getKey().array(), tokenRight);
                subMap = getPMTable().subMap(pdkLeft, includeStart, pdkRight, includeStop);
            }
            else
                subMap = getPMTable(); //TODO: This needs to be fixed. Need to add LocalToken support
        }
        return new PMTable.PMTableUnfilteredPartitionIterator(subMap.entrySet().iterator(), metadata, getTableMetadata(), doesClusteringKeyExist(), columnFilter, dataRange);
    }


    public static class PMTableUnfilteredPartitionIterator implements MTableUnfilteredPartitionIterator
    {
        private final Iterator<Map.Entry<PersistentDecoratedKey, AnyPersistent>> iter;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;
        private TableMetadata tableMetadata;
        private MTableMetadata mTableMetadata;
        private boolean isClusteringKeyAvailable;

        private PMTableUnfilteredPartitionIterator(Iterator<Map.Entry<PersistentDecoratedKey, AnyPersistent>> iter,
                                                   TableMetadata tableMetadata,
                                                   MTableMetadata mTableMetadata,
                                                   boolean isClusteringKeyAvailable,
                                                   ColumnFilter columnFilter,
                                                   DataRange dataRange)
        {
            this.iter = iter;
            this.tableMetadata = tableMetadata;
            this.mTableMetadata = mTableMetadata;
            this.isClusteringKeyAvailable = isClusteringKeyAvailable;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        public static <T> PMTableUnfilteredPartitionIterator getInstance(T iter,
                                                                         TableMetadata tableMetadata,
                                                                         MTableMetadata mTableMetadata,
                                                                         boolean isClusteringKeyAvailable,
                                                                         ColumnFilter columnFilter,
                                                                         DataRange dataRange)
        {
            return new PMTableUnfilteredPartitionIterator((Iterator<Map.Entry<PersistentDecoratedKey, AnyPersistent>>) iter, tableMetadata,
                                                          mTableMetadata, isClusteringKeyAvailable, columnFilter, dataRange);
        }

        @Override
        public TableMetadata metadata()
        {
            return tableMetadata;
        }

        @Override
        public boolean hasNext()
        {
            return iter.hasNext();
        }

        @Override
        public UnfilteredRowIterator next()
        {
            Map.Entry<PersistentDecoratedKey, AnyPersistent> entry = iter.next();
            assert entry != null;
            PersistentDecoratedKey pKey = entry.getKey();
            IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
            DecoratedKey key = partitioner.decorateKey(ByteBuffer.wrap(entry.getKey().getKey()));
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);


            if (isClusteringKeyAvailable)
            {
                if (filter.getSlices(tableMetadata) != Slices.ALL) //Read slices
                {
                    PCFFilter cfFilter = new PCFFilter(tableMetadata);
                    return cfFilter.getUnfilteredIterator(filter, (PersistentColumnFamilySortedMap) entry.getValue(), tableMetadata, mTableMetadata, key);
                }
                else //Read entire partition
                {
                    return new PMTableUnfilteredRowIterator(ByteBuffer.wrap(pKey.getKey()),
                                                            tableMetadata,
                                                            mTableMetadata,
                                                            ((PersistentColumnFamilySortedMap) entry.getValue()));
                }
            }
            else
            {
                return new PMTableUnfilteredRowIterator(ByteBuffer.wrap(pKey.getKey()),
                                                        tableMetadata,
                                                        mTableMetadata,
                                                        ((PersistentRowSingle) entry.getValue()));
            }
        }
    }
}
