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

import java.util.EnumSet;

import static java.util.EnumSet.of;

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
  APPEND,
  REPLACE,
  CLIENT_INVALIDATION_ACK,
  CLIENT_INVALIDATION_ALL_ACK,
  CLEAR,
  GET_STORE,
  LOCK,
  UNLOCK,
  ITERATOR_OPEN,
  ITERATOR_CLOSE,
  ITERATOR_ADVANCE,
  ENABLE_EVENT_LISTENER,

  // StateRepository operation messages
  GET_STATE_REPO,
  PUT_IF_ABSENT,
  ENTRY_SET,

  // Passive replication messages
  CHAIN_REPLICATION_OP,
  CLEAR_INVALIDATION_COMPLETE,
  INVALIDATION_COMPLETE,
  MESSAGE_CATCHUP;

  public static final EnumSet<EhcacheMessageType> LIFECYCLE_MESSAGES = of(VALIDATE, VALIDATE_SERVER_STORE, PREPARE_FOR_DESTROY);
  public static boolean isLifecycleMessage(EhcacheMessageType value) {
    return LIFECYCLE_MESSAGES.contains(value);
  }

  public static final EnumSet<EhcacheMessageType> STORE_OPERATION_MESSAGES = of(GET_AND_APPEND, APPEND,
          REPLACE, CLIENT_INVALIDATION_ACK, CLIENT_INVALIDATION_ALL_ACK, CLEAR, GET_STORE, LOCK, UNLOCK, ITERATOR_OPEN, ITERATOR_CLOSE, ITERATOR_ADVANCE, ENABLE_EVENT_LISTENER);
  public static boolean isStoreOperationMessage(EhcacheMessageType value) {
    return STORE_OPERATION_MESSAGES.contains(value);
  }

  public static final EnumSet<EhcacheMessageType> STATE_REPO_OPERATION_MESSAGES = of(GET_STATE_REPO, PUT_IF_ABSENT, ENTRY_SET);
  public static boolean isStateRepoOperationMessage(EhcacheMessageType value) {
    return STATE_REPO_OPERATION_MESSAGES.contains(value);
  }

  /**
   * All not idempotent messages are tracked. One exception is {@link #CLEAR}. It is idempotent but also a pretty costly operation so we prefer to avoid
   * to do it twice. The same list if used for passive and active. However, of course, according to the {@code EhcacheExecutionStrategy}, the following will happen
   * <ul>
   *   <li>{@link #CHAIN_REPLICATION_OP}: Received by the passive. This message will be transformed to look like the original GET_AND_APPEND and its response</li>
   *   <li>{@link #PUT_IF_ABSENT}: Received by both</li>
   *   <li>{@link #GET_AND_APPEND}: Received by the active (which will then send a passive replication message to the passive)</li>
   *   <li>{@link #APPEND}: Received by the active (which will then send a passive replication message to the passive)</li>
   *   <li>{@link #CLEAR}: Received by both</li>
   * </ul>
   */
  public static final EnumSet<EhcacheMessageType> TRACKED_OPERATION_MESSAGES = of(CHAIN_REPLICATION_OP, PUT_IF_ABSENT, GET_AND_APPEND, APPEND, CLEAR);
  public static boolean isTrackedOperationMessage(EhcacheMessageType value) {
    return TRACKED_OPERATION_MESSAGES.contains(value);
  }

  public static final EnumSet<EhcacheMessageType> PASSIVE_REPLICATION_MESSAGES = of(CHAIN_REPLICATION_OP, CLEAR_INVALIDATION_COMPLETE, INVALIDATION_COMPLETE);
  public static boolean isPassiveReplicationMessage(EhcacheMessageType value) {
    return PASSIVE_REPLICATION_MESSAGES.contains(value);
  }
}
