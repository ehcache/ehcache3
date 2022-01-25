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
  CONFIGURE,
  VALIDATE,
  CREATE_SERVER_STORE,
  VALIDATE_SERVER_STORE,
  RELEASE_SERVER_STORE,
  DESTROY_SERVER_STORE,

  // ServerStore operation messages
  GET_AND_APPEND,
  APPEND,
  REPLACE,
  CLIENT_INVALIDATION_ACK,
  CLEAR,
  GET_STORE,

  // StateRepository operation messages
  GET_STATE_REPO,
  PUT_IF_ABSENT,
  ENTRY_SET,

  // Passive replication messages
  CHAIN_REPLICATION_OP,
  CLIENT_ID_TRACK_OP,
  CLEAR_INVALIDATION_COMPLETE,
  INVALIDATION_COMPLETE,
  CREATE_SERVER_STORE_REPLICATION,
  DESTROY_SERVER_STORE_REPLICATION;

  public static final String MESSAGE_TYPE_FIELD_NAME = "opCode";
  public static final int MESSAGE_TYPE_FIELD_INDEX = 10;
  public static final EnumMapping<EhcacheMessageType> EHCACHE_MESSAGE_TYPES_ENUM_MAPPING = newEnumMappingBuilder(EhcacheMessageType.class)
    .mapping(CONFIGURE, 1)
    .mapping(VALIDATE, 2)
    .mapping(CREATE_SERVER_STORE, 3)
    .mapping(VALIDATE_SERVER_STORE, 4)
    .mapping(RELEASE_SERVER_STORE, 5)
    .mapping(DESTROY_SERVER_STORE, 6)

    .mapping(GET_AND_APPEND, 21)
    .mapping(APPEND, 22)
    .mapping(REPLACE, 23)
    .mapping(CLIENT_INVALIDATION_ACK, 24)
    .mapping(CLEAR, 25)
    .mapping(GET_STORE, 26)

    .mapping(GET_STATE_REPO, 41)
    .mapping(PUT_IF_ABSENT, 42)
    .mapping(ENTRY_SET, 43)

    .mapping(CHAIN_REPLICATION_OP, 61)
    .mapping(CLIENT_ID_TRACK_OP, 62)
    .mapping(CLEAR_INVALIDATION_COMPLETE, 63)
    .mapping(INVALIDATION_COMPLETE, 64)
    .mapping(CREATE_SERVER_STORE_REPLICATION, 65)
    .mapping(DESTROY_SERVER_STORE_REPLICATION, 66)
    .build();

  public static final EnumSet<EhcacheMessageType> LIFECYCLE_MESSAGES = of(CONFIGURE, VALIDATE, CREATE_SERVER_STORE, VALIDATE_SERVER_STORE, RELEASE_SERVER_STORE, DESTROY_SERVER_STORE);
  public static boolean isLifecycleMessage(EhcacheMessageType value) {
    return LIFECYCLE_MESSAGES.contains(value);
  }

  public static final EnumSet<EhcacheMessageType> STORE_OPERATION_MESSAGES = of(GET_AND_APPEND, APPEND, REPLACE, CLIENT_INVALIDATION_ACK, CLEAR, GET_STORE);
  public static boolean isStoreOperationMessage(EhcacheMessageType value) {
    return STORE_OPERATION_MESSAGES.contains(value);
  }

  public static final EnumSet<EhcacheMessageType> STATE_REPO_OPERATION_MESSAGES = of(GET_STATE_REPO, PUT_IF_ABSENT, ENTRY_SET);
  public static boolean isStateRepoOperationMessage(EhcacheMessageType value) {
    return STATE_REPO_OPERATION_MESSAGES.contains(value);
  }

  public static final EnumSet<EhcacheMessageType> PASSIVE_REPLICATION_MESSAGES = of(CHAIN_REPLICATION_OP, CLIENT_ID_TRACK_OP, CLEAR_INVALIDATION_COMPLETE, INVALIDATION_COMPLETE, CREATE_SERVER_STORE_REPLICATION, DESTROY_SERVER_STORE_REPLICATION);
  public static boolean isPassiveReplicationMessage(EhcacheMessageType value) {
    return PASSIVE_REPLICATION_MESSAGES.contains(value);
  }
}
