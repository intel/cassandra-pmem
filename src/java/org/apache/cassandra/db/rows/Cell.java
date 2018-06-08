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
package org.apache.cassandra.db.rows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;

//import lib.util.persistent.PersistentCellByteArray;
import lib.util.persistent.PersistentImmutableByteArray;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * A cell is our atomic unit for a single value of a single column.
 * <p>
 * A cell always holds at least a timestamp that gives us how the cell reconcile. We then
 * have 3 main types of cells:
 *   1) live regular cells: those will also have a value and, if for a complex column, a path.
 *   2) expiring cells: on top of regular cells, those have a ttl and a local deletion time (when they are expired).
 *   3) tombstone cells: those won't have value, but they have a local deletion time (when the tombstone was created).
 */
public abstract class Cell extends ColumnData
{
    public static final int NO_TTL = 0;
    public static final int NO_DELETION_TIME = Integer.MAX_VALUE;

    public final static Comparator<Cell> comparator = (c1, c2) ->
    {
        int cmp = c1.column().compareTo(c2.column());
        if (cmp != 0)
            return cmp;

        Comparator<CellPath> pathComparator = c1.column().cellPathComparator();
        return pathComparator == null ? 0 : pathComparator.compare(c1.path(), c2.path());
    };

    public static final Serializer serializer = new BufferCell.Serializer();

    public static final PMCellWriter cellWriter = new BufferCell.PMCellWriter();

    protected Cell(ColumnMetadata column)
    {
        super(column);
    }

    /**
     * Whether the cell is a counter cell or not.
     *
     * @return whether the cell is a counter cell or not.
     */
    public abstract boolean isCounterCell();

    /**
     * The cell value.
     *
     * @return the cell value.
     */
    public abstract ByteBuffer value();

    /**
     * The cell timestamp.
     * <p>
     * @return the cell timestamp.
     */
    public abstract long timestamp();

    /**
     * The cell ttl.
     *
     * @return the cell ttl, or {@code NO_TTL} if the cell isn't an expiring one.
     */
    public abstract int ttl();

    /**
     * The cell local deletion time.
     *
     * @return the cell local deletion time, or {@code NO_DELETION_TIME} if the cell is neither
     * a tombstone nor an expiring one.
     */
    public abstract int localDeletionTime();

    /**
     * Whether the cell is a tombstone or not.
     *
     * @return whether the cell is a tombstone or not.
     */
    public abstract boolean isTombstone();

    /**
     * Whether the cell is an expiring one or not.
     * <p>
     * Note that this only correspond to whether the cell liveness info
     * have a TTL or not, but doesn't tells whether the cell is already expired
     * or not. You should use {@link #isLive} for that latter information.
     *
     * @return whether the cell is an expiring one or not.
     */
    public abstract boolean isExpiring();

    /**
     * Whether the cell is live or not given the current time.
     *
     * @param nowInSec the current time in seconds. This is used to
     * decide if an expiring cell is expired or live.
     * @return whether the cell is live or not at {@code nowInSec}.
     */
    public abstract boolean isLive(int nowInSec);

    /**
     * For cells belonging to complex types (non-frozen collection and UDT), the
     * path to the cell.
     *
     * @return the cell path for cells of complex column, and {@code null} for other cells.
     */
    public abstract CellPath path();

    public abstract Cell withUpdatedColumn(ColumnMetadata newColumn);

    public abstract Cell withUpdatedValue(ByteBuffer newValue);

    public abstract Cell copy(AbstractAllocator allocator);

    @Override
    // Overrides super type to provide a more precise return type.
    public abstract Cell markCounterLocalToBeCleared();

    @Override
    // Overrides super type to provide a more precise return type.
    public abstract Cell purge(DeletionPurger purger, int nowInSec);

    public static class PMCellWriter {
        public final static int IS_DELETED_MASK             = 0x01; // Whether the cell is a tombstone or not.
        public final static int IS_EXPIRING_MASK            = 0x02; // Whether the cell is expiring.
        public final static int HAS_EMPTY_VALUE_MASK        = 0x04; // Wether the cell has an empty value. This will be the case for tombstone in particular.
        public final static int USE_ROW_TIMESTAMP_MASK      = 0x08; // Wether the cell has the same timestamp than the row this is a cell of.
        public final static int USE_ROW_TTL_MASK            = 0x10; // Wether the cell has the same ttl than the row this is a cell of.
        EncodingStats stats = EncodingStats.NO_STATS;
     //   private long minTimestamp = Long.MAX_VALUE;
        public boolean isLive(int localDeletionTime, int ttl)
        {
            int nowInSec = FBUtilities.nowInSeconds();
            return localDeletionTime == NO_DELETION_TIME || (ttl != NO_TTL && nowInSec < localDeletionTime);
        }

        // Writes cell data to the memory depending on what is enabled, volatile or persistent
        //public PersistentImmutableByteArray writeCelltoMemory(Cell cell, ColumnMetadata column, LivenessInfo rowLiveness, PersistentImmutableByteArray existingCell,EncodingStats stats)
        public byte[] writeCelltoMemory(Cell cell, ColumnMetadata column, LivenessInfo rowLiveness, PersistentImmutableByteArray existingCell,EncodingStats stats)
        {

            assert cell != null;
            this.stats = stats;
            boolean hasValue = cell.value().hasRemaining();
            boolean isDeleted = cell.isTombstone();
            boolean isExpiring = cell.isExpiring();
            boolean useRowTimestamp = !rowLiveness.isEmpty() && cell.timestamp() == rowLiveness.timestamp();
            boolean useRowTTL = isExpiring && rowLiveness.isExpiring() && cell.ttl() == rowLiveness.ttl() && cell.localDeletionTime() == rowLiveness.localExpirationTime();
            int flags = 0;
            if (!hasValue)
                flags |= HAS_EMPTY_VALUE_MASK;
            if (isDeleted)
                flags |= IS_DELETED_MASK;
            else if (isExpiring)
                flags |= IS_EXPIRING_MASK;
            if (useRowTimestamp)
                flags |= USE_ROW_TIMESTAMP_MASK;
            if (useRowTTL)
                flags |= USE_ROW_TTL_MASK;

            int size = getCellPayloadSize(cell, column, rowLiveness);
            byte[] cellp = null;
            short cellPathLen = 0;
            if (column.isComplex())
            {
                cell.path().get(0).mark();
                cellp = new byte[cell.path().get(0).remaining()];
                cell.path().get(0).get(cellp);
                cell.path().get(0).reset();
                cellPathLen = (short) cellp.length;
            }

            byte[] val = null;
            if (hasValue)
            {
                val = new byte[cell.value().remaining()];
                cell.value().duplicate().get(val);
            }

            long updateTimestamp = !useRowTimestamp ? cell.timestamp() : 0;
            int updateLocalDeletionTime = ((isDeleted || isExpiring) && !useRowTTL) ? cell.localDeletionTime() : Cell.NO_DELETION_TIME;
            int updateTtl = (isExpiring && !useRowTTL) ? cell.ttl() : Cell.NO_TTL;
            if(existingCell != null)
            {
                ByteBuffer existingCellBB = ByteBuffer.wrap(existingCell.toArray());
                int existingFlag = existingCellBB.get();

                boolean isDeletedExistingCell = (existingFlag & IS_DELETED_MASK) != 0;
                boolean isExpiringExistingCell = (existingFlag & IS_EXPIRING_MASK) != 0;
                boolean useRowTimestampExistingCell = (existingFlag & USE_ROW_TIMESTAMP_MASK) != 0;
                boolean useRowTTLExistingCell = (existingFlag & USE_ROW_TTL_MASK) != 0;

                long existingTimestamp = 0;
                if (!useRowTimestampExistingCell)
                {
                    existingTimestamp = existingCellBB.getInt();
                  /*  byte firstByte = existingCellBB.get();

                    //Bail out early if this is one byte, necessary or it fails later
                    if (firstByte >= 0)
                    {
                        existingTimestamp = firstByte;//add timestamp
                    }

                    else
                    {
                        int extraBytes = VIntCoding.numberOfExtraBytesToRead(firstByte);

                        int position = existingCellBB.position();
                        int extraBits = extraBytes * 8;

                        long retval = existingCellBB.getLong(position);
                        if (existingCellBB.order() == ByteOrder.LITTLE_ENDIAN)
                            retval = Long.reverseBytes(retval);
                        existingCellBB.position(position + extraBytes);

                        // truncate the bytes we read in excess of those we needed
                        retval >>>= 64 - extraBits;
                        // remove the non-value bits from the first byte
                        firstByte &= VIntCoding.firstByteValueMask(extraBytes);
                        // shift the first byte up to its correct position
                        existingTimestamp |= (long) firstByte << extraBits;
                    }*/
                }
                  //  existingTimestamp = existingCellBB.getLong();
                if (updateTimestamp != existingTimestamp)
                {
                    if (updateTimestamp < existingTimestamp) return null;
                    else
                    {
                        int existingLocalDeletionTime = Cell.NO_DELETION_TIME;
                        int existingTtl = Cell.NO_TTL;
                        if ((isDeletedExistingCell || isExpiringExistingCell) && !useRowTTLExistingCell)
                            existingLocalDeletionTime = existingCellBB.getInt();
                        if (isExpiringExistingCell && !useRowTTLExistingCell)
                            existingTtl = existingCellBB.getInt();
                        boolean isUpdateLive = isLive(updateLocalDeletionTime, updateTtl);
                        boolean isExistingLive = isLive(existingLocalDeletionTime, existingTtl);
                        if (isUpdateLive != isExistingLive)
                        {
                            if (isUpdateLive) return null;
                            else
                            {
                                int length = existingCellBB.getInt();
                                byte[] buf = new byte[length];
                                existingCellBB.get(buf, 0, length);
                                int c = ByteBuffer.wrap(buf).compareTo(cell.value());
                                if (c < 0) return null;

                                if (c == 0)
                                {
                                    if (updateLocalDeletionTime < existingLocalDeletionTime) return null;
                                }
                            }
                        }
                    }
                }

            }

            // Create immutable persistent buffer

            ByteBuffer bb = ByteBuffer.allocate(size + 4);
            bb.putInt(size);
            bb.put((byte) flags);

            Long value = updateTimestamp-stats.minTimestamp;

            bb.putInt(value.intValue());

            if ((isDeleted || isExpiring) && !useRowTTL)
                bb.putInt(updateLocalDeletionTime);
            if (isExpiring && !useRowTTL)
                bb.putInt(updateTtl);

            if (hasValue)
            {
                //    column
                if (column.type.valueLengthIfFixed() < 0)
                {
                    int valueLen = val.length;
                    bb.putInt(valueLen);
                }
                bb.put(val);
            }
            if(column.isComplex())
            {
                bb.putInt(cellPathLen);
                bb.put(cellp);
            }


            return bb.array();

        }
        private int getCellPayloadSize(Cell cell, ColumnMetadata column, LivenessInfo rowLiveness)
        {
            boolean hasValue = cell.value().hasRemaining();
            boolean isDeleted = cell.isTombstone();
            boolean isExpiring = cell.isExpiring();
         //   boolean useRowTimestamp = !rowLiveness.isEmpty() && cell.timestamp() == rowLiveness.timestamp();
            boolean useRowTTL = isExpiring && rowLiveness.isExpiring() && cell.ttl() == rowLiveness.ttl() && cell.localDeletionTime() == rowLiveness.localExpirationTime();


            int size = 1; //contains flags by default
            size += Integer.BYTES; //timestamp size

            if ((isDeleted || isExpiring) && !useRowTTL)
                size += TypeSizes.sizeofUnsignedVInt(cell.localDeletionTime() - stats.minLocalDeletionTime);
            if (isExpiring && !useRowTTL)
                size += TypeSizes.sizeofUnsignedVInt(cell.ttl() - stats.minTTL);
            if (column.isComplex())
            {
                int cellPathSize = cell.path().get(0).remaining();
                size +=Integer.BYTES +cellPathSize;
            }
            if (hasValue)
            {
                int cellSize = cell.value().remaining();
                if (column.type.valueLengthIfFixed() < 0)
                    size += Integer.BYTES;
                size += cellSize;
            }
            return size;
        }


    }

    /**
     * The serialization format for cell is:
     *     [ flags ][ timestamp ][ deletion time ][    ttl    ][ path size ][ path ][ value size ][ value ]
     *     [   1b  ][ 8b (vint) ][   4b (vint)   ][ 4b (vint) ][ 4b (vint) ][  arb ][  4b (vint) ][  arb  ]
     *
     * where not all field are always present (in fact, only the [ flags ] are guaranteed to be present). The fields have the following
     * meaning:
     *   - [ flags ] is the cell flags. It is a byte for which each bit represents a flag whose meaning is explained below (*_MASK constants)
     *   - [ timestamp ] is the cell timestamp. Present unless the cell has the USE_TIMESTAMP_MASK.
     *   - [ deletion time]: the local deletion time for the cell. Present if either the cell is deleted (IS_DELETED_MASK)
     *       or it is expiring (IS_EXPIRING_MASK) but doesn't have the USE_ROW_TTL_MASK.
     *   - [ ttl ]: the ttl for the cell. Present if the row is expiring (IS_EXPIRING_MASK) but doesn't have the
     *       USE_ROW_TTL_MASK.
     *   - [ value size ] is the size of the [ value ] field. It's present unless either the cell has the HAS_EMPTY_VALUE_MASK, or the value
     *       for columns of this type have a fixed length.
     *   - [ path size ] is the size of the [ path ] field. Present iff this is the cell of a complex column.
     *   - [ value ]: the cell value, unless it has the HAS_EMPTY_VALUE_MASK.
     *   - [ path ]: the cell path if the column this is a cell of is complex.
     */
    static class Serializer
    {
        private final static int IS_DELETED_MASK             = 0x01; // Whether the cell is a tombstone or not.
        private final static int IS_EXPIRING_MASK            = 0x02; // Whether the cell is expiring.
        private final static int HAS_EMPTY_VALUE_MASK        = 0x04; // Wether the cell has an empty value. This will be the case for tombstone in particular.
        private final static int USE_ROW_TIMESTAMP_MASK      = 0x08; // Wether the cell has the same timestamp than the row this is a cell of.
        private final static int USE_ROW_TTL_MASK            = 0x10; // Wether the cell has the same ttl than the row this is a cell of.

        public void serialize(Cell cell, ColumnMetadata column, DataOutputPlus out, LivenessInfo rowLiveness, SerializationHeader header) throws IOException
        {
            assert cell != null;
            boolean hasValue = cell.value().hasRemaining();
            boolean isDeleted = cell.isTombstone();
            boolean isExpiring = cell.isExpiring();
            boolean useRowTimestamp = !rowLiveness.isEmpty() && cell.timestamp() == rowLiveness.timestamp();
            boolean useRowTTL = isExpiring && rowLiveness.isExpiring() && cell.ttl() == rowLiveness.ttl() && cell.localDeletionTime() == rowLiveness.localExpirationTime();
            int flags = 0;
            if (!hasValue)
                flags |= HAS_EMPTY_VALUE_MASK;

            if (isDeleted)
                flags |= IS_DELETED_MASK;
            else if (isExpiring)
                flags |= IS_EXPIRING_MASK;

            if (useRowTimestamp)
                flags |= USE_ROW_TIMESTAMP_MASK;
            if (useRowTTL)
                flags |= USE_ROW_TTL_MASK;

            out.writeByte((byte)flags);

            if (!useRowTimestamp)
                header.writeTimestamp(cell.timestamp(), out);

            if ((isDeleted || isExpiring) && !useRowTTL)
                header.writeLocalDeletionTime(cell.localDeletionTime(), out);
            if (isExpiring && !useRowTTL)
                header.writeTTL(cell.ttl(), out);

            if (column.isComplex()) {
                column.cellPathSerializer().serialize(cell.path(), out);
            }

            if (hasValue) {
                header.getType(column).writeValue(cell.value(), out);
            }
        }

        public Cell deserialize(DataInputPlus in, LivenessInfo rowLiveness, ColumnMetadata column, SerializationHeader header, SerializationHelper helper) throws IOException
        {
            int flags = in.readUnsignedByte();
            boolean hasValue = (flags & HAS_EMPTY_VALUE_MASK) == 0;
            boolean isDeleted = (flags & IS_DELETED_MASK) != 0;
            boolean isExpiring = (flags & IS_EXPIRING_MASK) != 0;
            boolean useRowTimestamp = (flags & USE_ROW_TIMESTAMP_MASK) != 0;
            boolean useRowTTL = (flags & USE_ROW_TTL_MASK) != 0;

            long timestamp = useRowTimestamp ? rowLiveness.timestamp() : header.readTimestamp(in);

            int localDeletionTime = useRowTTL
                                    ? rowLiveness.localExpirationTime()
                                    : (isDeleted || isExpiring ? header.readLocalDeletionTime(in) : NO_DELETION_TIME);

            int ttl = useRowTTL ? rowLiveness.ttl() : (isExpiring ? header.readTTL(in) : NO_TTL);

            CellPath path = column.isComplex()
                            ? column.cellPathSerializer().deserialize(in)
                            : null;

            ByteBuffer value = ByteBufferUtil.EMPTY_BYTE_BUFFER;
            if (hasValue)
            {
                if (helper.canSkipValue(column) || (path != null && helper.canSkipValue(path)))
                {
                    header.getType(column).skipValue(in);
                }
                else
                {
                    boolean isCounter = localDeletionTime == NO_DELETION_TIME && column.type.isCounter();

                    value = header.getType(column).readValue(in, DatabaseDescriptor.getMaxValueSize());
                    if (isCounter)
                        value = helper.maybeClearCounterValue(value);
                }
            }

            return new BufferCell(column, timestamp, ttl, localDeletionTime, value, path);
        }

        public long serializedSize(Cell cell, ColumnMetadata column, LivenessInfo rowLiveness, SerializationHeader header)
        {
            long size = 1; // flags
            boolean hasValue = cell.value().hasRemaining();
            boolean isDeleted = cell.isTombstone();
            boolean isExpiring = cell.isExpiring();
            boolean useRowTimestamp = !rowLiveness.isEmpty() && cell.timestamp() == rowLiveness.timestamp();
            boolean useRowTTL = isExpiring && rowLiveness.isExpiring() && cell.ttl() == rowLiveness.ttl() && cell.localDeletionTime() == rowLiveness.localExpirationTime();

            if (!useRowTimestamp)
                size += header.timestampSerializedSize(cell.timestamp());

            if ((isDeleted || isExpiring) && !useRowTTL)
                size += header.localDeletionTimeSerializedSize(cell.localDeletionTime());
            if (isExpiring && !useRowTTL)
                size += header.ttlSerializedSize(cell.ttl());

            if (column.isComplex())
                size += column.cellPathSerializer().serializedSize(cell.path());

            if (hasValue)
                size += header.getType(column).writtenLength(cell.value());

            return size;
        }

        // Returns if the skipped cell was an actual cell (i.e. it had its presence flag).
        public boolean skip(DataInputPlus in, ColumnMetadata column, SerializationHeader header) throws IOException
        {
            int flags = in.readUnsignedByte();
            boolean hasValue = (flags & HAS_EMPTY_VALUE_MASK) == 0;
            boolean isDeleted = (flags & IS_DELETED_MASK) != 0;
            boolean isExpiring = (flags & IS_EXPIRING_MASK) != 0;
            boolean useRowTimestamp = (flags & USE_ROW_TIMESTAMP_MASK) != 0;
            boolean useRowTTL = (flags & USE_ROW_TTL_MASK) != 0;

            if (!useRowTimestamp)
                header.skipTimestamp(in);

            if (!useRowTTL && (isDeleted || isExpiring))
                header.skipLocalDeletionTime(in);

            if (!useRowTTL && isExpiring)
                header.skipTTL(in);

            if (column.isComplex())
                column.cellPathSerializer().skip(in);

            if (hasValue)
                header.getType(column).skipValue(in);

            return true;
        }
    }
}
