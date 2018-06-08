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

import lib.util.persistent.ObjectPointer;
import lib.util.persistent.PersistentArrayList;
import lib.util.persistent.PersistentImmutableByteArray;
import lib.util.persistent.PersistentImmutableObject;
import lib.util.persistent.types.IntField;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.memory.MClusteringKey;
import org.apache.cassandra.memory.MUtils;
import org.apache.cassandra.utils.FastByteOperations;

/**
 * An instance of this class represents the clustering key information for a specific row.
 * It includes column names, data types and values. Depending on the data type of each column
 * included in clustering key, the comparision happens to keep rows sorted within a partition.
 */
public final class PersistentClusteringKey extends PersistentArrayList<ClusteringKeyHolder> implements Comparable<PersistentClusteringKey>, MClusteringKey
{
    private static final ObjectType<PersistentClusteringKey> TYPE = ObjectType.extendClassWith(PersistentClusteringKey.class, PersistentArrayList.class);
    private static final int defaultSize = 10;

    // constructor
    public PersistentClusteringKey(int initialSize)
    {
        super(TYPE, initialSize);
    }

    public PersistentClusteringKey()
    {
        this(defaultSize);
    }

    // reconstructor
    @SuppressWarnings("unused")
    private PersistentClusteringKey(ObjectPointer<PersistentClusteringKey> pointer)
    {
        super(pointer);
    }

    @Override
    public byte[] getValueBuf(int pos)
    {
        return get(pos).getValue();
    }

    @Override
    public int getNumKeys()
    {
        return size();
    }

    @Override
    public void addClusteringKeyColumn(AbstractType<?> dataType, byte[] value)
    {
        if (dataType == null)
        {
            System.out.println("Nothing to add in clustering key.");
            return;
        }
        add(new ClusteringKeyHolder(MUtils.getPMDataType(dataType, dataType.isMultiCell()), value));
    }

    @Override
    public int compareTo(PersistentClusteringKey o)
    {
        // compare lengths first. Ideally it should never happen!!!
        int length = size();
        if (length < o.size())
        {
            return -1;
        }

        else if (length > o.size())
        {
            return 1;
        }
        int cmp = -1;
        for (int i = 0; i < length; i++)
        {
            ClusteringKeyHolder keyHolder = get(i);
            byte dataType = ByteBuffer.allocate(4).putInt(keyHolder.getDataTypeConst()).get(0);
            if (dataType == MUtils.BOOLEAN_TYPE)
            {
                ByteBuffer b1 = ByteBuffer.wrap(keyHolder.getValue());
                ByteBuffer b2 = ByteBuffer.wrap(o.get(i).getValue());
                cmp = BooleanType.instance.compareCustom(b1, b2);
            }
            else if (dataType == MUtils.INTEGER_TYPE)
            {
                ByteBuffer b1 = ByteBuffer.wrap(keyHolder.getValue());
                ByteBuffer b2 = ByteBuffer.wrap(o.get(i).getValue());
                cmp = IntegerType.instance.compareCustom(b1, b2);
            }
            else if (dataType == MUtils.DOUBLE_TYPE)
            {
                ByteBuffer b1 = ByteBuffer.wrap(keyHolder.getValue());
                ByteBuffer b2 = ByteBuffer.wrap(o.get(i).getValue());
                cmp = DoubleType.instance.compareCustom(b1, b2);
            }
            else if (dataType == MUtils.FLOAT_TYPE)
            {
                ByteBuffer b1 = ByteBuffer.wrap(keyHolder.getValue());
                ByteBuffer b2 = ByteBuffer.wrap(o.get(i).getValue());
                cmp = FloatType.instance.compareCustom(b1, b2);
            }
            else if (dataType == MUtils.TIMEUUID_TYPE)
            {
                ByteBuffer b1 = ByteBuffer.wrap(keyHolder.getValue());
                ByteBuffer b2 = ByteBuffer.wrap(o.get(i).getValue());
                cmp = TimeUUIDType.instance.compareCustom(b1, b2);
            }
            else if (dataType == MUtils.LONG_TYPE)
            {
                ByteBuffer b1 = ByteBuffer.wrap(keyHolder.getValue());
                ByteBuffer b2 = ByteBuffer.wrap(o.get(i).getValue());
                cmp = LongType.instance.compareCustom(b1, b2);
            }
            else
            {
                byte[] b1 = keyHolder.getValue();
                byte[] b2 = o.get(i).getValue();
                cmp = FastByteOperations.compareUnsigned(b1, 0, b1.length, b2, 0, b2.length);
            }
            if (cmp != 0)
                break;
        }
        return cmp;
    }

    public int hashCode()
    {
        return get(0).hashCode();
    }

    public boolean equals(Object obj)
    {
        // sanity checks
        if (obj == null)
            return false;
        else if (this == obj)
            return true;
        else if (getClass() != obj.getClass())
            return false;

        return compareTo((PersistentClusteringKey) obj) == 0 ? true : false;
    }
}

class ClusteringKeyHolder extends PersistentImmutableObject
{
    private static final IntField DATA_TYPE = new IntField();
    private static final ObjectField<PersistentImmutableByteArray> VALUE = new ObjectField<>();

    private static final ObjectType<ClusteringKeyHolder> TYPE = ObjectType.fromFields(ClusteringKeyHolder.class,
                                                                                      DATA_TYPE,
                                                                                      VALUE);

    public ClusteringKeyHolder(int dataType, byte[] value)
    {
        super(TYPE, (ClusteringKeyHolder self) ->
        {
            self.initIntField(DATA_TYPE, dataType);
            self.initObjectField(VALUE, new PersistentImmutableByteArray(value));
        });
    }

    // reconstructor

    @SuppressWarnings("unused")
    private ClusteringKeyHolder(ObjectPointer<? extends ClusteringKeyHolder> pointer)
    {
        super(pointer);
    }

    int getDataTypeConst()
    {
        return getIntField(DATA_TYPE);
    }

    public byte[] getValue()
    {
        return getObjectField(VALUE).toArray();
    }
}