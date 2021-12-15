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

package org.ehcache.clustered.common.internal.lock;

import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;

public class LockMessaging {

  private static final MessageCodec<LockOperation, LockTransition> CODEC = new MessageCodec<LockOperation, LockTransition>() {
    @Override
    public byte[] encodeMessage(LockOperation message) {
      return new byte[] {
        (byte) message.getOperation().ordinal(),
        (byte) message.getHoldType().ordinal()
      };
    }

    @Override
    public LockOperation decodeMessage(byte[] bytes) {
      return new LockOperation(Operation.values()[bytes[0]], HoldType.values()[bytes[1]]);
    }

    @Override
    public byte[] encodeResponse(LockTransition response) {
      if (response.isAcquired()) {
        return new byte[] {0x00};
      } else if (response.isReleased()) {
        return new byte[] {0x01};
      } else {
        return new byte[] {0x02};
      }
    }

    @Override
    public LockTransition decodeResponse(byte[] bytes) throws MessageCodecException {
      switch (bytes[0]) {
        case 0x00: return new LockTransition(true, false);
        case 0x01: return new LockTransition(false, true);
        case 0x02: return new LockTransition(false, false);
        default: throw new MessageCodecException("Unexpected transition encoding", null);
      }
    }
  };

  public static MessageCodec<LockOperation, LockTransition> codec() {
    return CODEC;
  }

  public static LockOperation tryLock(HoldType type) {
    return new LockOperation(Operation.TRY_ACQUIRE, type);
  }

  public static LockOperation lock(HoldType type) {
    return new LockOperation(Operation.ACQUIRE, type);
  }

  public static LockOperation unlock(HoldType type) {
    return new LockOperation(Operation.RELEASE, type);
  }

  public static LockTransition acquired() {
    return new LockTransition(true, false);
  }

  public static LockTransition released() {
    return new LockTransition(false, true);
  }

  public static LockTransition empty() {
    return new LockTransition(false, false);
  }

  public static class LockOperation implements EntityMessage {

    private final Operation operation;
    private final HoldType type;

    LockOperation(Operation operation, HoldType type) {
      this.operation = operation;
      this.type = type;
    }

    public Operation getOperation() {
      return operation;
    }

    public HoldType getHoldType() {
      return type;
    }
  }

  public static class LockTransition implements EntityResponse {

    private final boolean acquired;
    private final boolean released;

    private LockTransition(boolean acquired, boolean released) {
      this.acquired = acquired;
      this.released = released;
    }

    public boolean isAcquired() {
      return acquired;
    }

    public boolean isReleased() {
      return released;
    }
  }

  public enum HoldType {
    WRITE, READ
  }

  public enum Operation {
    ACQUIRE, TRY_ACQUIRE, RELEASE
  }
}
