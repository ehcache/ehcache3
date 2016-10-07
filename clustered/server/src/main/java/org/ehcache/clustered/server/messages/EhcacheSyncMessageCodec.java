/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.clustered.server.messages;

import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.store.Util;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.SyncMessageCodec;

import java.nio.ByteBuffer;

public class EhcacheSyncMessageCodec implements SyncMessageCodec<EhcacheEntityMessage> {

  private static final byte OPCODE_SIZE = 1;

  @Override
  public byte[] encode(final int concurrencyKey, final EhcacheEntityMessage message) throws MessageCodecException {
    if (message instanceof EntityStateSyncMessage) {
      byte[] encodedMsg = Util.marshall(message);
      ByteBuffer buffer = ByteBuffer.allocate(OPCODE_SIZE + encodedMsg.length);
      buffer.put(message.getOpCode());
      buffer.put(encodedMsg);
      return buffer.array();
    } else {
      throw new IllegalArgumentException(this.getClass().getName() + " can not encode " + message + " which is not a " + EntityStateSyncMessage.class);
    }
  }

  @Override
  public EhcacheEntityMessage decode(final int concurrencyKey, final byte[] payload) throws MessageCodecException {
    ByteBuffer message = ByteBuffer.wrap(payload);
    byte[] encodedMsg = new byte[message.capacity() - OPCODE_SIZE];
    byte opCode = message.get();
    if (opCode == EhcacheEntityMessage.Type.SYNC_OP.getCode()) {
      message.get(encodedMsg, 0, encodedMsg.length);
      EntityStateSyncMessage entityMessage = (EntityStateSyncMessage) Util.unmarshall(encodedMsg);
      return entityMessage;
    } else {
      throw new IllegalArgumentException("EntityStateSyncMessage operation not defined for : " + opCode);
    }
  }
}
