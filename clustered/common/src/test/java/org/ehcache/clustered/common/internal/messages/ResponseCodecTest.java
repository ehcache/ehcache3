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

import org.ehcache.clustered.common.internal.exceptions.IllegalMessageException;
import org.ehcache.clustered.common.internal.store.Chain;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ResponseCodecTest {

  private static final EhcacheEntityResponseFactory RESPONSE_FACTORY = new EhcacheEntityResponseFactory();
  private static final ResponseCodec RESPONSE_CODEC = new ResponseCodec();
  private static final long KEY = 42L;
  private static final int INVALIDATION_ID = 134;

  @Test
  public void testFailureResponseCodec() {
    EhcacheEntityResponse failure = RESPONSE_FACTORY.failure(new IllegalMessageException("Test Exception"));

    EhcacheEntityResponse decoded = RESPONSE_CODEC.decode(RESPONSE_CODEC.encode(failure));

    assertThat(((EhcacheEntityResponse.Failure)decoded).getCause().getMessage(), is("Test Exception"));
  }

  @Test
  public void testGetResponseCodec() {
    EhcacheEntityResponse getResponse = RESPONSE_FACTORY.response(getChain(false,
        createPayload(1L), createPayload(11L), createPayload(111L)));

    EhcacheEntityResponse decoded = RESPONSE_CODEC.decode(RESPONSE_CODEC.encode(getResponse));

    Chain decodedChain = ((EhcacheEntityResponse.GetResponse) decoded).getChain();

    Util.assertChainHas(decodedChain, 1L, 11L, 111L);
  }

  @Test
  public void testMapValueCodec() throws Exception {
    Object subject = new Date();
    EhcacheEntityResponse mapValue = EhcacheEntityResponse.mapValue(subject);
    EhcacheEntityResponse.MapValue decoded =
        (EhcacheEntityResponse.MapValue) RESPONSE_CODEC.decode(RESPONSE_CODEC.encode(mapValue));
    assertThat(decoded.getValue(), equalTo(subject));
  }

  @Test
  public void testSuccess() throws Exception {
    byte[] encoded = RESPONSE_CODEC.encode(EhcacheEntityResponse.Success.INSTANCE);
    assertThat(RESPONSE_CODEC.decode(encoded), Matchers.<EhcacheEntityResponse>sameInstance(EhcacheEntityResponse.Success.INSTANCE));
  }

  @Test
  public void testHashInvalidationDone() throws Exception {
    EhcacheEntityResponse.HashInvalidationDone response = new EhcacheEntityResponse.HashInvalidationDone(KEY);
    byte[] encoded = RESPONSE_CODEC.encode(response);
    EhcacheEntityResponse.HashInvalidationDone decodedResponse = (EhcacheEntityResponse.HashInvalidationDone) RESPONSE_CODEC.decode(encoded);

    assertThat(decodedResponse.getResponseType(), is(EhcacheResponseType.HASH_INVALIDATION_DONE));
    assertThat(decodedResponse.getKey(), is(KEY));
  }

  @Test
  public void testAllInvalidationDone() throws Exception {
    EhcacheEntityResponse.AllInvalidationDone response = new EhcacheEntityResponse.AllInvalidationDone();

    byte[] encoded = RESPONSE_CODEC.encode(response);
    EhcacheEntityResponse.AllInvalidationDone decodedResponse = (EhcacheEntityResponse.AllInvalidationDone) RESPONSE_CODEC.decode(encoded);

    assertThat(decodedResponse.getResponseType(), is(EhcacheResponseType.ALL_INVALIDATION_DONE));
  }

  @Test
  public void testClientInvalidateHash() throws Exception {
    EhcacheEntityResponse.ClientInvalidateHash response = new EhcacheEntityResponse.ClientInvalidateHash(KEY, INVALIDATION_ID);
    byte[] encoded = RESPONSE_CODEC.encode(response);
    EhcacheEntityResponse.ClientInvalidateHash decodedResponse = (EhcacheEntityResponse.ClientInvalidateHash) RESPONSE_CODEC.decode(encoded);

    assertThat(decodedResponse.getResponseType(), is(EhcacheResponseType.CLIENT_INVALIDATE_HASH));
    assertThat(decodedResponse.getKey(), is(KEY));
    assertThat(decodedResponse.getInvalidationId(), is(INVALIDATION_ID));
  }

  @Test
  public void testClientInvalidateAll() throws Exception {
    EhcacheEntityResponse.ClientInvalidateAll response = new EhcacheEntityResponse.ClientInvalidateAll(INVALIDATION_ID);

    byte[] encoded = RESPONSE_CODEC.encode(response);
    EhcacheEntityResponse.ClientInvalidateAll decodedResponse = (EhcacheEntityResponse.ClientInvalidateAll) RESPONSE_CODEC.decode(encoded);

    assertThat(decodedResponse.getResponseType(), is(EhcacheResponseType.CLIENT_INVALIDATE_ALL));
    assertThat(decodedResponse.getInvalidationId(), is(INVALIDATION_ID));
  }

  @Test
  public void testServerInvalidateHash() throws Exception {
    EhcacheEntityResponse.ServerInvalidateHash response = new EhcacheEntityResponse.ServerInvalidateHash(KEY);

    byte[] encoded = RESPONSE_CODEC.encode(response);
    EhcacheEntityResponse.ServerInvalidateHash decodedResponse = (EhcacheEntityResponse.ServerInvalidateHash) RESPONSE_CODEC.decode(encoded);

    assertThat(decodedResponse.getResponseType(), is(EhcacheResponseType.SERVER_INVALIDATE_HASH));
    assertThat(decodedResponse.getKey(), is(KEY));
  }

  @Test
  public void testPrepareForDestroy() throws Exception {
    Set<String> storeIdentifiers = new HashSet<String>();
    storeIdentifiers.add("store1");
    storeIdentifiers.add("anotherStore");
    EhcacheEntityResponse.PrepareForDestroy response = new EhcacheEntityResponse.PrepareForDestroy(storeIdentifiers);

    byte[] encoded = RESPONSE_CODEC.encode(response);
    EhcacheEntityResponse.PrepareForDestroy decodedResponse = (EhcacheEntityResponse.PrepareForDestroy) RESPONSE_CODEC.decode(encoded);

    assertThat(decodedResponse.getResponseType(), is(EhcacheResponseType.PREPARE_FOR_DESTROY));
    assertThat(decodedResponse.getStores(), is(storeIdentifiers));
  }
}
