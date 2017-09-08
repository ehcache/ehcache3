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

import org.terracotta.runnel.EnumMapping;

import java.util.EnumSet;

import static java.util.EnumSet.of;
import static org.terracotta.runnel.EnumMappingBuilder.newEnumMappingBuilder;

/**
 * EhcacheMessageType
 *
 * Whenever you edit this, you must think about enum mapping and helper methods
 */
public enum EhcacheMessageType {
  // Lifecycle messages
  VALIDATE,
  VALIDATE_SERVER_STORE,
  PREPARE_FOR_DESTROY,

  // ServerStore operation messages
  GET_AND_APPEND,
  REPLACE,
  CLIENT_INVALIDATION_ACK,
  CLIENT_INVALIDATION_ALL_ACK,
  CLEAR,
  GET_STORE,

  // StateRepository operation messages
  GET_STATE_REPO,
  PUT_IF_ABSENT,
  ENTRY_SET,

  // Passive replication messages
  CHAIN_REPLICATION_OP,
  CLEAR_INVALIDATION_COMPLETE,
  INVALIDATION_COMPLETE;

  public static final String MESSAGE_TYPE_FIELD_NAME = "opCode";
  public static final int MESSAGE_TYPE_FIELD_INDEX = 10;
  public static final EnumMapping<EhcacheMessageType> EHCACHE_MESSAGE_TYPES_ENUM_MAPPING = newEnumMappingBuilder(EhcacheMessageType.class)
    .mapping(VALIDATE, 1)
    .mapping(VALIDATE_SERVER_STORE, 2)
    .mapping(PREPARE_FOR_DESTROY, 3)

    .mapping(GET_AND_APPEND, 21)
    .mapping(REPLACE, 23)
    .mapping(CLIENT_INVALIDATION_ACK, 24)
    .mapping(CLIENT_INVALIDATION_ALL_ACK, 25)
    .mapping(CLEAR, 26)
    .mapping(GET_STORE, 27)

    .mapping(GET_STATE_REPO, 41)
    .mapping(PUT_IF_ABSENT, 42)
    .mapping(ENTRY_SET, 43)

    .mapping(CHAIN_REPLICATION_OP, 61)
    .mapping(CLEAR_INVALIDATION_COMPLETE, 63)
    .mapping(INVALIDATION_COMPLETE, 64)
    .build();

  public static final EnumSet<EhcacheMessageType> LIFECYCLE_MESSAGES = of(VALIDATE, VALIDATE_SERVER_STORE, PREPARE_FOR_DESTROY);
  public static boolean isLifecycleMessage(EhcacheMessageType value) {
    return LIFECYCLE_MESSAGES.contains(value);
  }

  public static final EnumSet<EhcacheMessageType> STORE_OPERATION_MESSAGES = of(GET_AND_APPEND, REPLACE, CLIENT_INVALIDATION_ACK, CLIENT_INVALIDATION_ALL_ACK, CLEAR, GET_STORE);
  public static boolean isStoreOperationMessage(EhcacheMessageType value) {
    return STORE_OPERATION_MESSAGES.contains(value);
  }

  public static final EnumSet<EhcacheMessageType> STATE_REPO_OPERATION_MESSAGES = of(GET_STATE_REPO, PUT_IF_ABSENT, ENTRY_SET);
  public static boolean isStateRepoOperationMessage(EhcacheMessageType value) {
    return STATE_REPO_OPERATION_MESSAGES.contains(value);
  }

  /**
   * All not idempotent messages are tracked. One exception is {@link #CLEAR}. It is idempotent but also a pretty costly operation so we prefer to avoid
   * to do it twice.
   */
  public static final EnumSet<EhcacheMessageType> TRACKED_OPERATION_MESSAGES = of(GET_STATE_REPO, PUT_IF_ABSENT, ENTRY_SET, GET_AND_APPEND, CLEAR);
  public static boolean isTrackedOperationMessage(EhcacheMessageType value) {
    return TRACKED_OPERATION_MESSAGES.contains(value);
  }

  public static final EnumSet<EhcacheMessageType> PASSIVE_REPLICATION_MESSAGES = of(CHAIN_REPLICATION_OP, CLEAR_INVALIDATION_COMPLETE, INVALIDATION_COMPLETE);
  public static boolean isPassiveReplicationMessage(EhcacheMessageType value) {
    return PASSIVE_REPLICATION_MESSAGES.contains(value);
  }
}
