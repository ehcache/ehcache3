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

import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.store.Util;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.Enm;
import org.terracotta.runnel.decoding.StructDecoder;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.wrap;
import static org.ehcache.clustered.common.internal.messages.ChainCodec.CHAIN_ENCODER_FUNCTION;
import static org.ehcache.clustered.common.internal.messages.ChainCodec.CHAIN_STRUCT;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.AllInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.ClientInvalidateAll;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.ClientInvalidateHash;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.HashInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.ServerInvalidateHash;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.MapValue;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.EHCACHE_RESPONSE_TYPES_ENUM_MAPPING;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.RESPONSE_TYPE_FIELD_INDEX;
import static org.ehcache.clustered.common.internal.messages.EhcacheResponseType.RESPONSE_TYPE_FIELD_NAME;
import static org.ehcache.clustered.common.internal.messages.ExceptionCodec.EXCEPTION_ENCODER_FUNCTION;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.KEY_FIELD;
import static org.ehcache.clustered.common.internal.messages.MessageCodecUtils.SERVER_STORE_NAME_FIELD;

public class ResponseCodec {

  private static final String EXCEPTION_FIELD = "exception";
  private static final String INVALIDATION_ID_FIELD = "invalidationId";
  private static final String CHAIN_FIELD = "chain";
  private static final String MAP_VALUE_FIELD = "mapValue";

  private static final Struct SUCCESS_RESPONSE_STRUCT = StructBuilder.newStructBuilder()
    .enm(RESPONSE_TYPE_FIELD_NAME, RESPONSE_TYPE_FIELD_INDEX, EHCACHE_RESPONSE_TYPES_ENUM_MAPPING)
    .build();
  private static final Struct FAILURE_RESPONSE_STRUCT = StructBuilder.newStructBuilder()
    .enm(RESPONSE_TYPE_FIELD_NAME, RESPONSE_TYPE_FIELD_INDEX, EHCACHE_RESPONSE_TYPES_ENUM_MAPPING)
    .struct(EXCEPTION_FIELD, 20, ExceptionCodec.EXCEPTION_STRUCT)
    .build();
  private static final Struct GET_RESPONSE_STRUCT = StructBuilder.newStructBuilder()
    .enm(RESPONSE_TYPE_FIELD_NAME, RESPONSE_TYPE_FIELD_INDEX, EHCACHE_RESPONSE_TYPES_ENUM_MAPPING)
    .struct(CHAIN_FIELD, 20, CHAIN_STRUCT)
    .build();
  private static final Struct HASH_INVALIDATION_DONE_RESPONSE_STRUCT = StructBuilder.newStructBuilder()
    .enm(RESPONSE_TYPE_FIELD_NAME, RESPONSE_TYPE_FIELD_INDEX, EHCACHE_RESPONSE_TYPES_ENUM_MAPPING)
    .string(SERVER_STORE_NAME_FIELD, 20)
    .int64(KEY_FIELD, 30)
    .build();
  private static final Struct ALL_INVALIDATION_DONE_RESPONSE_STRUCT = StructBuilder.newStructBuilder()
    .enm(RESPONSE_TYPE_FIELD_NAME, RESPONSE_TYPE_FIELD_INDEX, EHCACHE_RESPONSE_TYPES_ENUM_MAPPING)
    .string(SERVER_STORE_NAME_FIELD, 20)
    .build();
  private static final Struct CLIENT_INVALIDATE_HASH_RESPONSE_STRUCT = StructBuilder.newStructBuilder()
    .enm(RESPONSE_TYPE_FIELD_NAME, RESPONSE_TYPE_FIELD_INDEX, EHCACHE_RESPONSE_TYPES_ENUM_MAPPING)
    .string(SERVER_STORE_NAME_FIELD, 20)
    .int64(KEY_FIELD, 30)
    .int32(INVALIDATION_ID_FIELD, 40)
    .build();
  private static final Struct CLIENT_INVALIDATE_ALL_RESPONSE_STRUCT = StructBuilder.newStructBuilder()
    .enm(RESPONSE_TYPE_FIELD_NAME, RESPONSE_TYPE_FIELD_INDEX, EHCACHE_RESPONSE_TYPES_ENUM_MAPPING)
    .string(SERVER_STORE_NAME_FIELD, 20)
    .int32(INVALIDATION_ID_FIELD, 30)
    .build();
  private static final Struct SERVER_INVALIDATE_HASH_RESPONSE_STRUCT = StructBuilder.newStructBuilder()
    .enm(RESPONSE_TYPE_FIELD_NAME, RESPONSE_TYPE_FIELD_INDEX, EHCACHE_RESPONSE_TYPES_ENUM_MAPPING)
    .string(SERVER_STORE_NAME_FIELD, 20)
    .int64(KEY_FIELD, 30)
    .build();
  private static final Struct MAP_VALUE_RESPONSE_STRUCT = StructBuilder.newStructBuilder()
    .enm(RESPONSE_TYPE_FIELD_NAME, RESPONSE_TYPE_FIELD_INDEX, EHCACHE_RESPONSE_TYPES_ENUM_MAPPING)
    .byteBuffer(MAP_VALUE_FIELD, 20)
    .build();

  public byte[] encode(EhcacheEntityResponse response) {
    switch (response.getResponseType()) {
      case FAILURE:
        final EhcacheEntityResponse.Failure failure = (EhcacheEntityResponse.Failure)response;
        return FAILURE_RESPONSE_STRUCT.encoder()
          .enm(RESPONSE_TYPE_FIELD_NAME, failure.getResponseType())
          .struct(EXCEPTION_FIELD, failure.getCause(), EXCEPTION_ENCODER_FUNCTION)
          .encode().array();
      case SUCCESS:
        return SUCCESS_RESPONSE_STRUCT.encoder()
          .enm(RESPONSE_TYPE_FIELD_NAME, response.getResponseType())
          .encode().array();
      case GET_RESPONSE:
        final EhcacheEntityResponse.GetResponse getResponse = (EhcacheEntityResponse.GetResponse)response;
        return GET_RESPONSE_STRUCT.encoder()
          .enm(RESPONSE_TYPE_FIELD_NAME, getResponse.getResponseType())
          .struct(CHAIN_FIELD, getResponse.getChain(), CHAIN_ENCODER_FUNCTION)
          .encode().array();
      case HASH_INVALIDATION_DONE: {
        HashInvalidationDone hashInvalidationDone = (HashInvalidationDone) response;
        return HASH_INVALIDATION_DONE_RESPONSE_STRUCT.encoder()
          .enm(RESPONSE_TYPE_FIELD_NAME, hashInvalidationDone.getResponseType())
          .string(SERVER_STORE_NAME_FIELD, hashInvalidationDone.getCacheId())
          .int64(KEY_FIELD, hashInvalidationDone.getKey())
          .encode().array();
      }
      case ALL_INVALIDATION_DONE: {
        AllInvalidationDone allInvalidationDone = (AllInvalidationDone) response;
        return ALL_INVALIDATION_DONE_RESPONSE_STRUCT.encoder()
          .enm(RESPONSE_TYPE_FIELD_NAME, allInvalidationDone.getResponseType())
          .string(SERVER_STORE_NAME_FIELD, allInvalidationDone.getCacheId())
          .encode().array();
      }
      case CLIENT_INVALIDATE_HASH: {
        ClientInvalidateHash clientInvalidateHash = (ClientInvalidateHash) response;
        return CLIENT_INVALIDATE_HASH_RESPONSE_STRUCT.encoder()
          .enm(RESPONSE_TYPE_FIELD_NAME, clientInvalidateHash.getResponseType())
          .string(SERVER_STORE_NAME_FIELD, clientInvalidateHash.getCacheId())
          .int64(KEY_FIELD, clientInvalidateHash.getKey())
          .int32(INVALIDATION_ID_FIELD, clientInvalidateHash.getInvalidationId())
          .encode().array();
      }
      case CLIENT_INVALIDATE_ALL: {
        ClientInvalidateAll clientInvalidateAll = (ClientInvalidateAll) response;
        return CLIENT_INVALIDATE_ALL_RESPONSE_STRUCT.encoder()
          .enm(RESPONSE_TYPE_FIELD_NAME, clientInvalidateAll.getResponseType())
          .string(SERVER_STORE_NAME_FIELD, clientInvalidateAll.getCacheId())
          .int32(INVALIDATION_ID_FIELD, clientInvalidateAll.getInvalidationId())
          .encode().array();
      }
      case SERVER_INVALIDATE_HASH: {
        ServerInvalidateHash serverInvalidateHash = (ServerInvalidateHash) response;
        return SERVER_INVALIDATE_HASH_RESPONSE_STRUCT.encoder()
          .enm(RESPONSE_TYPE_FIELD_NAME, serverInvalidateHash.getResponseType())
          .string(SERVER_STORE_NAME_FIELD, serverInvalidateHash.getCacheId())
          .int64(KEY_FIELD, serverInvalidateHash.getKey())
          .encode().array();
      }
      case MAP_VALUE: {
        MapValue mapValue = (MapValue) response;
        byte[] encodedMapValue = Util.marshall(mapValue.getValue());
        return MAP_VALUE_RESPONSE_STRUCT.encoder()
          .enm(RESPONSE_TYPE_FIELD_NAME, mapValue.getResponseType())
          .byteBuffer(MAP_VALUE_FIELD, wrap(encodedMapValue))
          .encode().array();
      }
      default:
        throw new UnsupportedOperationException("The operation is not supported : " + response.getResponseType());
    }
  }

  public EhcacheEntityResponse decode(byte[] payload) {
    ByteBuffer buffer = wrap(payload);
    StructDecoder<Void> decoder = SUCCESS_RESPONSE_STRUCT.decoder(buffer);
    Enm<EhcacheResponseType> opCodeEnm = decoder.enm(RESPONSE_TYPE_FIELD_NAME);

    if (!opCodeEnm.isFound()) {
      throw new AssertionError("Got a response without an opCode");
    }
    if (!opCodeEnm.isValid()) {
      // Need to ignore the response here as we do not understand its type - coming from the future?
      return null;
    }

    EhcacheResponseType opCode = opCodeEnm.get();
    buffer.rewind();
    switch (opCode) {
      case SUCCESS:
        return EhcacheEntityResponse.Success.INSTANCE;
      case FAILURE:
        decoder = FAILURE_RESPONSE_STRUCT.decoder(buffer);
        ClusterException exception = ExceptionCodec.decode(decoder.struct(EXCEPTION_FIELD));
        return new EhcacheEntityResponse.Failure(exception.withClientStackTrace());
      case GET_RESPONSE:
        decoder = GET_RESPONSE_STRUCT.decoder(buffer);
        return new EhcacheEntityResponse.GetResponse(ChainCodec.decode(decoder.struct(CHAIN_FIELD)));
      case HASH_INVALIDATION_DONE: {
        decoder = HASH_INVALIDATION_DONE_RESPONSE_STRUCT.decoder(buffer);
        String cacheId = decoder.string(SERVER_STORE_NAME_FIELD);
        long key = decoder.int64(KEY_FIELD);
        return EhcacheEntityResponse.hashInvalidationDone(cacheId, key);
      }
      case ALL_INVALIDATION_DONE: {
        decoder = ALL_INVALIDATION_DONE_RESPONSE_STRUCT.decoder(buffer);
        String cacheId = decoder.string(SERVER_STORE_NAME_FIELD);
        return EhcacheEntityResponse.allInvalidationDone(cacheId);
      }
      case CLIENT_INVALIDATE_HASH: {
        decoder = CLIENT_INVALIDATE_HASH_RESPONSE_STRUCT.decoder(buffer);
        String cacheId = decoder.string(SERVER_STORE_NAME_FIELD);
        long key = decoder.int64(KEY_FIELD);
        int invalidationId = decoder.int32(INVALIDATION_ID_FIELD);
        return EhcacheEntityResponse.clientInvalidateHash(cacheId, key, invalidationId);
      }
      case CLIENT_INVALIDATE_ALL: {
        decoder = CLIENT_INVALIDATE_ALL_RESPONSE_STRUCT.decoder(buffer);
        String cacheId = decoder.string(SERVER_STORE_NAME_FIELD);
        int invalidationId = decoder.int32(INVALIDATION_ID_FIELD);
        return EhcacheEntityResponse.clientInvalidateAll(cacheId, invalidationId);
      }
      case SERVER_INVALIDATE_HASH: {
        decoder = SERVER_INVALIDATE_HASH_RESPONSE_STRUCT.decoder(buffer);
        String cacheId = decoder.string(SERVER_STORE_NAME_FIELD);
        long key = decoder.int64(KEY_FIELD);
        return EhcacheEntityResponse.serverInvalidateHash(cacheId, key);
      }
      case MAP_VALUE: {
        decoder = MAP_VALUE_RESPONSE_STRUCT.decoder(buffer);
        return EhcacheEntityResponse.mapValue(Util.unmarshall(decoder.byteBuffer(MAP_VALUE_FIELD)));
      }
      default:
        throw new UnsupportedOperationException("The operation is not supported with opCode : " + opCode);
    }
  }
}
