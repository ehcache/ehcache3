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

import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.APPEND;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.CHAIN_REPLICATION_OP;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.CLEAR;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.CLEAR_INVALIDATION_COMPLETE;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.CLIENT_INVALIDATION_ACK;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.CLIENT_INVALIDATION_ALL_ACK;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.ENABLE_EVENT_LISTENER;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.ENTRY_SET;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.GET_AND_APPEND;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.GET_STATE_REPO;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.GET_STORE;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.INVALIDATION_COMPLETE;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.ITERATOR_ADVANCE;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.ITERATOR_CLOSE;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.ITERATOR_OPEN;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.LOCK;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.MESSAGE_CATCHUP;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.PUT_IF_ABSENT;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.REPLACE;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.UNLOCK;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.VALIDATE;
import static org.ehcache.clustered.common.internal.messages.EhcacheMessageType.VALIDATE_SERVER_STORE;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.ALL_INVALIDATION_DONE;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.CLIENT_INVALIDATE_ALL;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.CLIENT_INVALIDATE_HASH;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.FAILURE;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.GET_RESPONSE;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.HASH_INVALIDATION_DONE;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.ITERATOR_BATCH;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.LOCK_FAILURE;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.LOCK_SUCCESS;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.MAP_VALUE;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.RESOLVE_REQUEST;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.SERVER_APPEND;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.SERVER_INVALIDATE_HASH;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.SUCCESS;
import static org.terracotta.runnel.EnumMappingBuilder.newEnumMappingBuilder;

public class BaseCodec {

  public static final String MESSAGE_TYPE_FIELD_NAME = "opCode";
  public static final int MESSAGE_TYPE_FIELD_INDEX = 10;
  public static final EnumMapping<EhcacheMessageType> EHCACHE_MESSAGE_TYPES_ENUM_MAPPING = newEnumMappingBuilder(EhcacheMessageType.class)
    .mapping(VALIDATE, 1)
    .mapping(VALIDATE_SERVER_STORE, 2)
    .mapping(EhcacheMessageType.PREPARE_FOR_DESTROY, 3)

    .mapping(GET_AND_APPEND, 21)
    .mapping(APPEND, 22)
    .mapping(REPLACE, 23)
    .mapping(CLIENT_INVALIDATION_ACK, 24)
    .mapping(CLIENT_INVALIDATION_ALL_ACK, 25)
    .mapping(CLEAR, 26)
    .mapping(GET_STORE, 27)
    .mapping(LOCK, 28)
    .mapping(UNLOCK, 29)
    .mapping(ITERATOR_OPEN, 30)
    .mapping(ITERATOR_CLOSE, 31)
    .mapping(ITERATOR_ADVANCE, 32)
    .mapping(ENABLE_EVENT_LISTENER, 33)

    .mapping(GET_STATE_REPO, 41)
    .mapping(PUT_IF_ABSENT, 42)
    .mapping(ENTRY_SET, 43)

    .mapping(CHAIN_REPLICATION_OP, 61)
    .mapping(CLEAR_INVALIDATION_COMPLETE, 63)
    .mapping(INVALIDATION_COMPLETE, 64)

    .mapping(MESSAGE_CATCHUP, 71)
    .build();

  public static final String RESPONSE_TYPE_FIELD_NAME = "opCode";
  public static final int RESPONSE_TYPE_FIELD_INDEX = 10;
  public static final EnumMapping<EhcacheResponseType> EHCACHE_RESPONSE_TYPES_ENUM_MAPPING = newEnumMappingBuilder(EhcacheResponseType.class)
    .mapping(SUCCESS, 80)
    .mapping(FAILURE, 81)
    .mapping(GET_RESPONSE, 82)
    .mapping(HASH_INVALIDATION_DONE, 83)
    .mapping(ALL_INVALIDATION_DONE, 84)
    .mapping(CLIENT_INVALIDATE_HASH, 85)
    .mapping(CLIENT_INVALIDATE_ALL, 86)
    .mapping(SERVER_INVALIDATE_HASH, 87)
    .mapping(MAP_VALUE, 88)
    .mapping(EhcacheResponseType.PREPARE_FOR_DESTROY, 89)
    .mapping(RESOLVE_REQUEST, 90)
    .mapping(LOCK_SUCCESS, 91)
    .mapping(LOCK_FAILURE, 92)
    .mapping(ITERATOR_BATCH, 93)
    .mapping(SERVER_APPEND, 94)
    .build();

}
