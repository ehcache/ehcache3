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

package org.ehcache.clustered.server.internal.messages;

import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;

import com.tc.classloader.CommonComponent;

import java.util.UUID;

@CommonComponent
public abstract class EntitySyncMessage extends EhcacheEntityMessage {

  @CommonComponent
  public enum SyncOp {

    STATE((byte) 31),
    DATA((byte) 32),
    ;

    private final byte syncOpCode;

    SyncOp(byte syncOpCode) {
      this.syncOpCode = syncOpCode;
    }

    public byte getOpCode() {
      return this.syncOpCode;
    }

    public static SyncOp getSyncOp(byte syncOpCode) {
      switch (syncOpCode) {
        case 31:
          return STATE;
        case 32:
          return DATA;
        default:
          throw new IllegalArgumentException("Sync operation not defined for : " + syncOpCode);
      }
    }

  }

  @Override
  public Type getType() {
    return Type.SYNC_OP;
  }

  public abstract SyncOp operation();

  @Override
  public byte getOpCode() {
    return operation().getOpCode();
  }

  @Override
  public void setId(final long id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public UUID getClientId() {
    throw new UnsupportedOperationException();
  }

}
