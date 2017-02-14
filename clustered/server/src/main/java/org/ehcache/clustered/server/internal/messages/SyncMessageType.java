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

import org.terracotta.runnel.EnumMapping;

import com.tc.classloader.CommonComponent;

import static org.terracotta.runnel.EnumMappingBuilder.newEnumMappingBuilder;

/**
 * SyncMessageType
 */
@CommonComponent
public enum SyncMessageType {
  STATE_REPO,
  DATA;

  public static final String SYNC_MESSAGE_TYPE_FIELD_NAME = "msgType";
  public static final int SYNC_MESSAGE_TYPE_FIELD_INDEX = 10;
  public static final EnumMapping<SyncMessageType> SYNC_MESSAGE_TYPE_MAPPING = newEnumMappingBuilder(SyncMessageType.class)
    .mapping(STATE_REPO, 1)
    .mapping(DATA, 10)
    .build();
}
