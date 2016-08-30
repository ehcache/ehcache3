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

package org.ehcache.clustered.common.internal.messages;

import org.terracotta.entity.EntityMessage;

/**
 * Defines messages for interactions with an {@code EhcacheActiveEntity}.
 */
public abstract class EhcacheEntityMessage implements EntityMessage {

  /**
   * These types represent the top level Ehcache entity message types.
   * Each of these top level types can have subtypes of messages.
   * The byte code values represents the upper bound of the subtypes messages' byte values if there are any.
   */
  public enum Type {
    LIFECYCLE_OP((byte) 10),
    SERVER_STORE_OP((byte) 20),
    STATE_REPO_OP((byte) 30),
    ;

    private final byte code;

    Type(byte code) {
      this.code = code;
    }

    public byte getCode() {
      return this.code;
    }

    public static Type toType(byte code) {
      for (Type type: Type.values()) {
        if(type.getCode() == code) {
          return type;
        }
      }
      throw new IllegalArgumentException("Invalid message type code: " + code);
    }
  }

  public abstract Type getType();

  public abstract byte getOpCode();

  @Override
  public String toString() {
    return getType().toString();
  }
}
