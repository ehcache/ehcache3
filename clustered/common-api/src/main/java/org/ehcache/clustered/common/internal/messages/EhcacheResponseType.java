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
  ITERATOR_BATCH,
  SERVER_APPEND,
  ;
}
