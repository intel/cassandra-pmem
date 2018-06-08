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

import org.apache.cassandra.db.rows.UnfilteredRowIterator;

public interface MTableWriter
{

    /*
   * Unfiltered flags constants.
   */
    // TODO: future work
    // 1. Below flags have been ported from UnfilteredSerializer.java. Copied them here in order to minimize the changes
    // in the existing code. The ideal solution would be have them in a common placed shared by different classes.
    // 2. MTableWriter class code review and optimizations
    // field with that flag.
    public final static int HAS_TIMESTAMP = 0x04; // Whether the encoded row has a timestamp (i.e. if row
    // .partitionKeyLivenessInfo().hasTimestamp() == true).
    public final static int HAS_TTL = 0x08; // Whether the encoded row has some expiration info (i.e. if row
    // .partitionKeyLivenessInfo().hasTTL() == true).
    public final static int HAS_DELETION = 0x10; // Whether the encoded row has some deletion info.
    public final static int HAS_ALL_COLUMNS = 0x20; // Whether the encoded row has all of the columns from the
    // header present.
    public final static int HAS_COMPLEX_DELETION = 0x40; // Whether the encoded row has some complex deletion for at
    // least one of its columns.
    public final static int EXTENSION_FLAG = 0x80; // If present, another byte is read containing the "extended
    /*
     * Extended flags
     */
    public final static int IS_STATIC = 0x01; // Whether the encoded row is a static. If there is no extended flag,
    // flags" above.
    // the row is assumed not static.
    public final static int HAS_SHADOWABLE_DELETION = 0x02; // Whether the row deletion is shadowable. If there is
    // no extended flag (or no row deletion), the deletion is assumed not shadowable.

    public void write(UnfilteredRowIterator iterator);
}
