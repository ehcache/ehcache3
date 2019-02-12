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

import static org.terracotta.runnel.EnumMappingBuilder.newEnumMappingBuilder;

/**
 * EhcacheResponseType
 */
public enum EhcacheResponseType {
  SUCCESS,
  FAILURE,
  GET_RESPONSE,
  HASH_INVALIDATION_DONE,
  CLIENT_INVALIDATE_HASH,
  CLIENT_INVALIDATE_ALL,
  SERVER_INVALIDATE_HASH,
  MAP_VALUE,
  ALL_INVALIDATION_DONE,
  PREPARE_FOR_DESTROY,
  RESOLVE_REQUEST,
  LOCK_SUCCESS,
  LOCK_FAILURE,
  ITERATOR_BATCH;


  public static final String RESPONSE_TYPE_FIELD_NAME = "opCode";
  public static final int RESPONSE_TYPE_FIELD_INDEX = 10;
  public static final EnumMapping<EhcacheResponseType> EHCACHE_RESPONSE_TYPES_ENUM_MAPPING = newEnumMappingBuilder(EhcacheResponseType.class)
    .mapping(EhcacheResponseType.SUCCESS, 80)
    .mapping(EhcacheResponseType.FAILURE, 81)
    .mapping(EhcacheResponseType.GET_RESPONSE, 82)
    .mapping(EhcacheResponseType.HASH_INVALIDATION_DONE, 83)
    .mapping(EhcacheResponseType.ALL_INVALIDATION_DONE, 84)
    .mapping(EhcacheResponseType.CLIENT_INVALIDATE_HASH, 85)
    .mapping(EhcacheResponseType.CLIENT_INVALIDATE_ALL, 86)
    .mapping(EhcacheResponseType.SERVER_INVALIDATE_HASH, 87)
    .mapping(EhcacheResponseType.MAP_VALUE, 88)
    .mapping(EhcacheResponseType.PREPARE_FOR_DESTROY, 89)
    .mapping(EhcacheResponseType.RESOLVE_REQUEST, 90)
    .mapping(EhcacheResponseType.LOCK_SUCCESS, 91)
    .mapping(EhcacheResponseType.LOCK_FAILURE, 92)
    .mapping(EhcacheResponseType.ITERATOR_BATCH, 93)
    .build();
}
