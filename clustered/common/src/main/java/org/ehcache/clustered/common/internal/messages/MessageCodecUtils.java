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

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.terracotta.runnel.EnumMapping;
import org.terracotta.runnel.decoding.Enm;
import org.terracotta.runnel.decoding.PrimitiveDecodingSupport;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.PrimitiveEncodingSupport;
import org.terracotta.runnel.encoding.StructEncoder;

import java.util.UUID;

import static org.terracotta.runnel.EnumMappingBuilder.newEnumMappingBuilder;

/**
 * MessageCodecUtils
 */
public class MessageCodecUtils {

  public static final String MSG_ID_FIELD = "msgId";
  public static final String LSB_UUID_FIELD = "lsbUUID";
  public static final String MSB_UUID_FIELD = "msbUUID";
  public static final String SERVER_STORE_NAME_FIELD = "serverStoreName";
  public static final String KEY_FIELD = "key";

  public void encodeMandatoryFields(StructEncoder<Void> encoder, EhcacheOperationMessage message) {
    encoder.enm(EhcacheMessageType.MESSAGE_TYPE_FIELD_NAME, message.getMessageType())
      .int64(MSG_ID_FIELD, message.getId())
      .int64(MSB_UUID_FIELD, message.getClientId().getMostSignificantBits())
      .int64(LSB_UUID_FIELD, message.getClientId().getLeastSignificantBits());
  }

  public UUID decodeUUID(StructDecoder<Void> decoder) {
    return new UUID(decoder.int64(MSB_UUID_FIELD), decoder.int64(LSB_UUID_FIELD));
  }
}
