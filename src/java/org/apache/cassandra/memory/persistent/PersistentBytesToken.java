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
import lib.util.persistent.PersistentByteArray;
import lib.util.persistent.types.ObjectField;
import lib.util.persistent.types.ObjectType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;

public final class PersistentBytesToken extends PMToken
{
    private static final ObjectField<PersistentByteArray> BYTES_TOKEN = new ObjectField<>();

    private static final ObjectType<PersistentBytesToken> TYPE = ObjectType.fromFields(PersistentBytesToken .class,
                                                                                       BYTES_TOKEN);

    // constructor
    public PersistentBytesToken(byte[] token)
    {
        super(TYPE, (PersistentBytesToken self) ->
        {
            self.initObjectField(BYTES_TOKEN, new PersistentByteArray(token));
        });
    }

    // resonctructor
    @SuppressWarnings("unused")
    private PersistentBytesToken(ObjectPointer<? extends PersistentBytesToken> pointer)
    {
        super(pointer);
    }

    @Override
    public PersistentByteArray getToken()
    {
        return getObjectField(BYTES_TOKEN);
    }

    @Override
    public <T> int compareTo(T token)
    {
        byte[] token1 = getToken().toArray();
        byte[] token2 = ((PersistentBytesToken) token).getToken().toArray();
        int cmp = FBUtilities.compareUnsigned(token1, token2, 0, 0, token1.length, token2.length);
        return cmp;
    }

    // TODO: Not Tested
    @Override
    public int compareWith(Token bytesToken)
    {
        byte[] token1 = getToken().toArray();
        byte[] token2 = (byte[]) bytesToken.getToken().getTokenValue();
        int cmp = FBUtilities.compareUnsigned(token1, token2, 0, 0, token1.length, token2.length);
        return cmp;
    }
}
