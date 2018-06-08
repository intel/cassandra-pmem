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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import lib.util.persistent.PersistentArrayList;
import lib.util.persistent.PersistentString;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.memory.persistent.PersistentColumnMetadata;
import org.apache.cassandra.memory.persistent.PersistentTableMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

public class MUtils
{

    public static final byte INVALID_TYPE = 0;
    public static final byte BOOLEAN_TYPE = 1;
    public static final byte TIMESTAMP_TYPE = 2;
    public static final byte INTEGER_TYPE = 3;
    public static final byte LONG_TYPE = 4;
    public static final byte STRING_TYPE = 5;
    public static final byte DOUBLE_TYPE = 6;
    public static final byte FLOAT_TYPE = 7;
    public static final byte TIMEUUID_TYPE = 8;
    public static final byte BLOB_TYPE = 9;
    public static final byte UTF8_TYPE = 10;
    public static final byte UUID_TYPE = 11;
    public static final byte BIGINT_TYPE = 12;
    public static final byte INET_ADDRESS_TYPE = 13;
    public static final byte MAP_TYPE = 14;
    public static final byte SET_TYPE = 15;
    public static final byte LIST_TYPE = 16;
    public static final byte EMPTY_TYPE = 17;

    private static int getIntDataType(AbstractType<?> type)
    {
        ByteBuffer buf = ByteBuffer.allocate(4);
        if (type instanceof UUIDType) buf.put(UUID_TYPE);
        else if (type instanceof InetAddressType) buf.put(INET_ADDRESS_TYPE);
        else if (type instanceof TimeUUIDType) buf.put(TIMEUUID_TYPE);
        else if (type instanceof TimestampType) buf.put(TIMESTAMP_TYPE);
        else if (type instanceof DoubleType) buf.put(DOUBLE_TYPE);
        else if (type instanceof FloatType) buf.put(FLOAT_TYPE);
        else if (type instanceof BooleanType) buf.put(BOOLEAN_TYPE);
        else if (type instanceof UTF8Type) buf.put(UTF8_TYPE);
        else if (type instanceof BytesType) buf.put(BLOB_TYPE);
        else if (type instanceof Int32Type) buf.put(INTEGER_TYPE);
        else if (type instanceof LongType) buf.put(LONG_TYPE);
        else if (type instanceof EmptyType) buf.put(EMPTY_TYPE);
        buf.rewind();
        return buf.getInt();
    }

    private static byte getByteDataType(AbstractType<?> type)
    {
        if (type instanceof UUIDType) return UUID_TYPE;
        else if (type instanceof InetAddressType) return INET_ADDRESS_TYPE;
        else if (type instanceof TimeUUIDType) return TIMEUUID_TYPE;
        else if (type instanceof TimestampType) return TIMESTAMP_TYPE;
        else if (type instanceof DoubleType) return DOUBLE_TYPE;
        else if (type instanceof FloatType) return FLOAT_TYPE;
        else if (type instanceof BooleanType) return BOOLEAN_TYPE;
        else if (type instanceof UTF8Type) return UTF8_TYPE;
        else if (type instanceof BytesType) return BLOB_TYPE;
        else if (type instanceof Int32Type) return INTEGER_TYPE;
        else if (type instanceof LongType) return LONG_TYPE;
        else if (type instanceof EmptyType) return EMPTY_TYPE;
        return INVALID_TYPE;
    }

    private static AbstractType<?> getTypeFromByte(byte type)
    {
        if (type == BOOLEAN_TYPE) return BooleanType.instance;
        else if (type == TIMESTAMP_TYPE) return TimestampType.instance;
        else if (type == INTEGER_TYPE) return Int32Type.instance;
        else if (type == LONG_TYPE) return LongType.instance;
        else if (type == STRING_TYPE) return UTF8Type.instance;
        else if (type == DOUBLE_TYPE) return DoubleType.instance;
        else if (type == FLOAT_TYPE) return FloatType.instance;
        else if (type == TIMEUUID_TYPE) return TimeUUIDType.instance;
        else if (type == BLOB_TYPE) return BytesType.instance;
        else if (type == UTF8_TYPE) return UTF8Type.instance;
        else if (type == UUID_TYPE) return UUIDType.instance;
        else if (type == BIGINT_TYPE) return IntegerType.instance;
        else if (type == INET_ADDRESS_TYPE) return InetAddressType.instance;
        else if (type == EMPTY_TYPE) return EmptyType.instance;
        return null;
    }

    public static int getPMDataType(AbstractType<?> dataType, boolean isMultiCell)
    {
        if (dataType instanceof MapType)
        {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.put(MAP_TYPE);
            buf.put(getByteDataType(((MapType) dataType).getKeysType()));
            buf.put(getByteDataType(((MapType) dataType).getValuesType()));
            buf.put(isMultiCell ? (byte) 1 : (byte) 0);
            buf.rewind();
            return buf.getInt();
        }
        else if (dataType instanceof SetType)
        {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.put(SET_TYPE);
            buf.put(getByteDataType(((SetType) dataType).getElementsType()));
            buf.put((byte) 0);
            buf.put(isMultiCell ? (byte) 1 : (byte) 0);
            buf.rewind();
            return buf.getInt();
        }
        else if (dataType instanceof ListType)
        {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.put(LIST_TYPE);
            buf.put(getByteDataType(((ListType) dataType).getElementsType()));
            buf.put((byte) 0);
            buf.put(isMultiCell ? (byte) 1 : (byte) 0);
            buf.rewind();
            return buf.getInt();
        }
        else
            return getIntDataType(dataType);
    }

    public static AbstractType<?> getTypeFromPM(int type)
    {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(type);
        buf.rewind();
        byte pmDataType = buf.get();
        if (pmDataType == MAP_TYPE)
        {
            return MapType.getInstance(getTypeFromByte(buf.get()), getTypeFromByte(buf.get()), buf.get() == 1 ? true : false);
        }
        else if (pmDataType == SET_TYPE)
        {
            return SetType.getInstance(getTypeFromByte(buf.get()), buf.get(3) == 1 ? true : false);
        }
        else if (pmDataType == LIST_TYPE)
        {
            return ListType.getInstance(getTypeFromByte(buf.get()), buf.get(3) == 1 ? true : false);
        }
        else
            return getTypeFromByte(pmDataType);
    }

    public static TableMetadata getTableMetadataFromPM(MTableMetadata persistentTableMetadata)
    {
        TableMetadata.Builder builder =
        TableMetadata.builder(persistentTableMetadata.getKeyspace(), persistentTableMetadata.getTableName(),
                              TableId.fromUUID(new UUID(persistentTableMetadata.getTableId().getMostSignificantBits(),
                                                        persistentTableMetadata.getTableId().getLeastSignificantBits())));
        for (PersistentColumnMetadata columnMetadata : (PersistentArrayList<PersistentColumnMetadata>) persistentTableMetadata.getPartitionKey())
        {
            int dataType = columnMetadata.getPMDataType();
            String colName = columnMetadata.getColumnName();
            Schema.instance.storePersistentColumnName(colName, columnMetadata.getPColumnName());
            AbstractType<?> abstractType = MUtils.getTypeFromPM(dataType);
            assert abstractType != null;
            boolean isMultiCell = abstractType.isMultiCell();
            if (dataType == MUtils.MAP_TYPE)
            {
                AbstractType keys, values;
                MapType mapType = (MapType) abstractType;
                keys = mapType.getKeysType();
                values = mapType.getValuesType();
                builder.addPartitionKeyColumn(colName, MapType.getInstance(keys, values, isMultiCell));
            }
            else if (dataType == MUtils.SET_TYPE)
            {
                AbstractType elements;
                elements = ((SetType) abstractType).getElementsType();
                builder.addPartitionKeyColumn(colName, SetType.getInstance(elements, isMultiCell));
            }
            else if (dataType == MUtils.LIST_TYPE)
            {
                AbstractType values;
                values = ((ListType) abstractType).getElementsType();
                builder.addPartitionKeyColumn(colName, ListType.getInstance(values, isMultiCell));
            }
            else
                builder.addPartitionKeyColumn(colName, abstractType);
        }
        for (PersistentColumnMetadata columnMetadata :
        ((PersistentTableMetadata) persistentTableMetadata).getClusteringKey())
        {
            int dataType = columnMetadata.getPMDataType();
            String colName = columnMetadata.getColumnName();
            Schema.instance.storePersistentColumnName(colName, columnMetadata.getPColumnName());
            AbstractType<?> abstractType = MUtils.getTypeFromPM(dataType);
            assert abstractType != null;
            boolean isMultiCell = abstractType.isMultiCell();
            if (dataType == MUtils.MAP_TYPE)
            {
                AbstractType keys, values;
                MapType mapType = (MapType) abstractType;
                keys = mapType.getKeysType();
                values = mapType.getValuesType();
                builder.addClusteringColumn(colName, MapType.getInstance(keys, values, isMultiCell));
            }
            else if (dataType == MUtils.SET_TYPE)
            {
                AbstractType elements;
                elements = ((SetType) abstractType).getElementsType();
                builder.addClusteringColumn(colName, SetType.getInstance(elements, isMultiCell));
            }
            else if (dataType == MUtils.LIST_TYPE)
            {
                AbstractType values;
                values = ((ListType)abstractType).getElementsType();
                builder.addClusteringColumn(colName, ListType.getInstance(values, isMultiCell));
            }
            else
                builder.addClusteringColumn(colName, abstractType);
        }

        //TODO: Need to address static column
        for (Map.Entry<PersistentString, PersistentColumnMetadata> columnMetadata :
        ((PersistentTableMetadata) persistentTableMetadata).getRegularStaticColumns().entrySet())
        {
            int dataType = columnMetadata.getValue().getPMDataType();
            String colName = columnMetadata.getValue().getColumnName();
            Schema.instance.storePersistentColumnName(colName, columnMetadata.getValue().getPColumnName());
            AbstractType<?> abstractType = MUtils.getTypeFromPM(dataType);
            assert abstractType != null;
            boolean isMultiCell = abstractType.isMultiCell();
            if (dataType == MUtils.MAP_TYPE)
            {
                AbstractType keys, values;
                MapType mapType = (MapType) abstractType;
                keys = mapType.getKeysType();
                values = mapType.getValuesType();
                builder.addRegularColumn(colName, MapType.getInstance(keys, values, isMultiCell));
            }
            else if (dataType == MUtils.SET_TYPE)
            {
                AbstractType elements;
                elements = ((SetType) abstractType).getElementsType();
                builder.addRegularColumn(colName, SetType.getInstance(elements, isMultiCell));
            }
            else if (dataType == MUtils.LIST_TYPE)
            {
                AbstractType elements;
                elements = ((ListType) abstractType).getElementsType();
                builder.addRegularColumn(colName, ListType.getInstance(elements, isMultiCell));
            }
            else
                builder.addRegularColumn(colName, abstractType);
        }
        return builder.build();
    }
}
