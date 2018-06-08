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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;

public class MHeader
{
    public final AbstractType<?> keyType;
    public final List<AbstractType<?>> clusteringTypes;

    public RegularAndStaticColumns columns;

    public ColumnsCollector columnsCollector;

    public MHeader(AbstractType<?> keyType,
                   List<AbstractType<?>> clusteringTypes,
                   RegularAndStaticColumns columns)
    {
        this.keyType = keyType;
        this.clusteringTypes = clusteringTypes;
        this.columns = columns;
        this.columnsCollector = null;
    }

    public MHeader(AbstractType<?> keyType,
                   List<AbstractType<?>> clusteringTypes,
                   ColumnsCollector columnsCollector)
    {
        this(keyType, clusteringTypes, columnsCollector.get());
        this.columnsCollector = columnsCollector;
    }

    public Columns columns(boolean isStatic)
    {
        return isStatic ? columns.statics : columns.regulars;
    }

    public List<AbstractType<?>> clusteringTypes()
    {
        return clusteringTypes;
    }

    public AbstractType<?> getType(ColumnMetadata column)
    {
        return column.type;
    }

    public void update(RegularAndStaticColumns columns)
    {
        this.columnsCollector.update(columns);
        this.columns = this.columnsCollector.get();
    }

    public static class ColumnsCollector
    {
        private final HashMap<ColumnMetadata, AtomicBoolean> predefined = new HashMap<>();
        private final ConcurrentSkipListSet<ColumnMetadata> extra = new ConcurrentSkipListSet<>();

        public ColumnsCollector(RegularAndStaticColumns columns)
        {
            for (ColumnMetadata def : columns.statics)
            {
                predefined.put(def, new AtomicBoolean());
            }
            for (ColumnMetadata def : columns.regulars)
            {
                predefined.put(def, new AtomicBoolean());
            }
        }

        public void update(RegularAndStaticColumns columns)
        {
            for (ColumnMetadata s : columns.statics)
                update(s);
            for (ColumnMetadata r : columns.regulars)
                update(r);
        }

        private void update(ColumnMetadata definition)
        {
            AtomicBoolean present = predefined.get(definition);
            if (present != null)
            {
                if (!present.get())
                    present.set(true);
            }
            else
            {
                extra.add(definition);
            }
        }

        public RegularAndStaticColumns get()
        {
            RegularAndStaticColumns.Builder builder = RegularAndStaticColumns.builder();
            for (Map.Entry<ColumnMetadata, AtomicBoolean> e : predefined.entrySet())
            {
                if (e.getValue().get())
                {
                    builder.add(e.getKey());
                }
            }
            return builder.addAll(extra).build();
        }
    }
}
