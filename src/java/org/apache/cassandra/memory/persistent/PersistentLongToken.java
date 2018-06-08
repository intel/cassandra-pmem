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
import lib.util.persistent.types.LongField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.dht.Token;

public final class PersistentLongToken extends PMToken
{
    private static final LongField LONG_TOKEN = new LongField();

    private static final ObjectType<PersistentLongToken> TYPE = ObjectType.fromFields(PersistentLongToken.class,
                                                                                      LONG_TOKEN);

    // constructor
    public PersistentLongToken(long token)
    {
        super(TYPE, (PersistentLongToken self) ->
        {
            self.initLongField(LONG_TOKEN, token);
        });
    }

    // reconstructor
    @SuppressWarnings("unused")
    private PersistentLongToken(ObjectPointer<? extends PersistentLongToken> pointer)
    {
        super(pointer);
    }

    @Override
    public Long getToken()
    {
        return getLongField(LONG_TOKEN);
    }

    @Override
    public <T> int compareTo(T token)
    {
        int cmp = Long.compare(getToken(), ((PersistentLongToken) token).getToken());
        return cmp;
    }

    @Override
    public int compareWith(Token longToken)
    {
        int cmp = Long.compare(getToken(), (Long) longToken.getTokenValue());
        return cmp;
    }
}