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
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Consumer;

import com.google.common.primitives.Longs;

import lib.util.persistent.AnyPersistent;
//import lib.util.persistent.BitUtil;
import lib.util.persistent.ObjectPointer;
import lib.util.persistent.PersistentArrays;
import lib.util.persistent.PersistentByteArray;
import lib.util.persistent.PersistentObject;
import lib.util.persistent.PersistentSkipListMap;
import lib.util.persistent.PersistentString;
import lib.util.persistent.types.ByteField;
import lib.util.persistent.types.IntField;
import lib.util.persistent.types.LongField;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ObjectType;
import lib.util.persistent.types.ValueField;
import org.apache.cassandra.memory.MDeletionTime;
import org.apache.cassandra.memory.MRow;
import org.apache.hadoop.hdfs.util.ByteArray;

class PersistentRowMetadata extends PersistentObject
{
    private static final ByteField FLAGS = new ByteField();
    private static final ByteField EXTENDED_FLAGS = new ByteField();
    private static final LongField PK_LIVENESS_TIMESTAMP = new LongField();
    private static final IntField PRIMARY_KEY_TTL = new IntField();
    private static final IntField PK_LOCAL_EXPIRATION_TIME = new IntField();
    private static final ValueField<PersistentDeletionTime> DELETION_TIME = new ValueField<>(PersistentDeletionTime.class);

    private static final ObjectType<PersistentRowMetadata> TYPE = ObjectType.withValueFields(PersistentRowMetadata.class,
                                                                                             FLAGS,
                                                                                             EXTENDED_FLAGS,
                                                                                             PK_LIVENESS_TIMESTAMP,
                                                                                             PRIMARY_KEY_TTL,
                                                                                             PK_LOCAL_EXPIRATION_TIME,
                                                                                             DELETION_TIME);

    public PersistentRowMetadata(byte flags, byte extendedFlags, boolean hasTimestamp, long timestamp,
                                 boolean hasTtl, int ttl, int localExpTime, boolean hasDeletion,
                                 PersistentDeletionTime deletionTime)
    {
        super(TYPE);
        setByteField(FLAGS, flags);
        setByteField(EXTENDED_FLAGS, extendedFlags);
        if (hasTimestamp)
            setLongField(PK_LIVENESS_TIMESTAMP, timestamp);
        if (hasTtl)
        {
            setIntField(PRIMARY_KEY_TTL, ttl);
            setIntField(PK_LOCAL_EXPIRATION_TIME, localExpTime);
        }
        if (hasDeletion)
            setObjectField(DELETION_TIME, deletionTime);
    }

    // reconstructor
    @SuppressWarnings("unused")
    private PersistentRowMetadata(ObjectPointer<? extends PersistentRowMetadata> pointer)
    {
        super(pointer);
    }

    public byte getExtendedFlags()
    {
        return getByteField(EXTENDED_FLAGS);
    }

    public long getPrimaryKeyLivenessTimestamp()
    {
        return getLongField(PK_LIVENESS_TIMESTAMP);
    }

    public void setPrimaryKeyLivenessTimestamp(long primaryKeyLivenessTimestamp)
    {
        setLongField(PK_LIVENESS_TIMESTAMP, primaryKeyLivenessTimestamp);
    }

    public int getPkLocalExpirationTime()
    {
        return getIntField(PK_LOCAL_EXPIRATION_TIME);
    }

    public void setPkLocalExpirationTime(int pkLocalExpirationTime)
    {
        setIntField(PK_LOCAL_EXPIRATION_TIME, pkLocalExpirationTime);
    }

    public MDeletionTime getDeletionTime()
    {
        return getObjectField(DELETION_TIME);
    }

    public void setDeletionTime(PersistentDeletionTime deletionTime)
    {
        setObjectField(DELETION_TIME, deletionTime);
    }
}

public class PersistentRow extends PersistentObject implements MRow
{
    private static final ValueField<PersistentRowMetadata> ROW_METADATA = new ValueField<>(PersistentRowMetadata.class);
    private static final ObjectField<PersistentByteArray> CELLS =  new ObjectField<>();
   // private static final ObjectField<PersistentSkipListMap<PersistentString, AnyPersistent>> CELLS = new ObjectField<>();
    private static final ObjectType<PersistentRow> TYPE = ObjectType.fromFields(PersistentRow.class, ROW_METADATA, CELLS);

    // constructor
    public PersistentRow(byte flags, byte extendedFlags, boolean hasTimestamp, long timestamp,
                         boolean hasTtl, int ttl, int localExpTime, boolean hasDeletion,
                         PersistentDeletionTime deletionTime)
    {
        super(TYPE, (PersistentRow self) ->
        {
            self.initObjectField(ROW_METADATA, new PersistentRowMetadata(flags,
                                                                         extendedFlags,
                                                                         hasTimestamp, timestamp,
                                                                         hasTtl, ttl, localExpTime,
                                                                         hasDeletion, deletionTime));
            //self.initObjectField(CELLS, new PersistentSkipListMap<>());
        });
    }

    // reconstructor
    protected PersistentRow(ObjectPointer<? extends PersistentRow> pointer)
    {
        super(pointer);
    }

    // subclassing constructor
    protected <T extends PersistentRow> PersistentRow(ObjectType<? extends PersistentRow> type, byte flags, byte extendedFlags, boolean hasTimestamp, long timestamp,
                                                      boolean hasTtl, int ttl, int localExpTime, boolean hasDeletion,
                                                      PersistentDeletionTime deletionTime, Consumer<T> initializer)
    {
        super(type, (PersistentRow self) ->
        {
            self.initObjectField(ROW_METADATA, new PersistentRowMetadata(flags,
                                                                         extendedFlags,
                                                                         hasTimestamp, timestamp,
                                                                         hasTtl, ttl, localExpTime,
                                                                         hasDeletion, deletionTime));
            
            if (initializer != null) initializer.accept((T) self);
        });
    }

    public PersistentRowMetadata getMetadata()
    {
        return getObjectField(ROW_METADATA);
    }

    @Override
    public PersistentByteArray getCells()
    {
        return getObjectField(CELLS);
    }

   /* public void addCell(PersistentString cellName, AnyPersistent cell)
    {
        if (cell != null) getObjectField(CELLS).put(cellName, cell);
    }*/

    public void addCells(byte[] columnsBitmap, byte[][] cells, int length)
    {
        //getObjectField(CELLS).putAll(cells);
        //setObjectField(CELLS, new PersistentSkipListMap<>(cells));
        PersistentByteArray cellField = new PersistentByteArray(columnsBitmap.length + length);
        PersistentArrays.fromByteArray(columnsBitmap, 0, cellField, 0, columnsBitmap.length);
        int cellOffset = columnsBitmap.length;
        for (int i = 0; i < cells.length; i++) {
            PersistentArrays.fromByteArray(cells[i], 0, cellField, cellOffset, cells[i].length);
            cellOffset += cells[i].length;
        }
        setObjectField(CELLS, cellField);
    }

    public void updateCells(byte[] columnsBitmap, byte[][] newCells,int length, int noColumns)
    {
        int newCellArrayLength = length;
        byte[] existingCells = getCells().toArray();


        ByteBuffer existingBuff = ByteBuffer.wrap(existingCells);
        long existingBitmap = existingBuff.getLong();

        ByteBuffer newBB = ByteBuffer.wrap(columnsBitmap);
        long newBitmap = newBB.getLong();

        int newIndex = 0 ;
        long mergedBitmap = newBitmap | existingBitmap;
        int cellCount = noColumns;//Long.bitCount(mergedBitmap);
        //int cellCount = (mergedBitmap ==0)? noColumns: 64-Long.bitCount(mergedBitmap);

        byte[][] tempCell = new byte[cellCount][];

       // int cellIndex =0;
        for(int index =0 ;index < cellCount;)
        {
            if (((existingBitmap | newBitmap) & 1L) == 0L)
            {
                existingBitmap = existingBitmap >> 1;
                newBitmap = newBitmap >> 1;
                index++;
                continue;
            }

            if((newBitmap & 1L) == 1L)//address timestamp later
            {
                tempCell[index] = newCells[newIndex];

                newIndex++;

                if((existingBitmap & 1L) == 1L)
                {
                    int cellSize = existingBuff.getInt();
                    existingBuff.position(existingBuff.position()+cellSize);

                }
            }
            else
            {
                int cellSize = existingBuff.getInt(existingBuff.position());
                byte[] tempArray = new byte[cellSize+4];
                existingBuff.get(tempArray);
                tempCell[index] = tempArray;
                newCellArrayLength += cellSize + 4;

            }
            existingBitmap = existingBitmap >> 1;
            newBitmap = newBitmap >> 1;
            index++;

        }
        int cellOffset = 8;
        PersistentByteArray cellField = new PersistentByteArray(columnsBitmap.length + newCellArrayLength);
        PersistentArrays.fromByteArray(Longs.toByteArray(mergedBitmap), 0, cellField, 0, 8); // refactor later
        for (int i = 0; i < tempCell.length; i++) {
            if (tempCell[i] == null)
                continue;
            PersistentArrays.fromByteArray(tempCell[i], 0, cellField, cellOffset, tempCell[i].length);
            cellOffset += tempCell[i].length;
        }
        setObjectField(CELLS, cellField);

    }
}
