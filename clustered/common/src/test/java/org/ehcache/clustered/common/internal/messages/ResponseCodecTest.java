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

import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.allInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.clientInvalidateAll;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.clientInvalidateHash;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.failure;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.getResponse;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.hashInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.mapValue;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.prepareForDestroy;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.serverInvalidateHash;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.success;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ResponseCodecTest {

  private static final ResponseCodec RESPONSE_CODEC = new ResponseCodec();
  private static final long KEY = 42L;
  private static final int INVALIDATION_ID = 134;

  @Test
  public void testFailureResponseCodec() {
    EhcacheEntityResponse failure = failure(new IllegalMessageException("Test Exception"));

    EhcacheEntityResponse decoded = RESPONSE_CODEC.decode(RESPONSE_CODEC.encode(failure));

    assertThat(((EhcacheEntityResponse.Failure)decoded).getCause().getMessage(), is("Test Exception"));
  }

  @Test
  public void testGetResponseCodec() {
    EhcacheEntityResponse getResponse = getResponse(getChain(false,
        createPayload(1L), createPayload(11L), createPayload(111L)));

    EhcacheEntityResponse decoded = RESPONSE_CODEC.decode(RESPONSE_CODEC.encode(getResponse));

    Chain decodedChain = ((EhcacheEntityResponse.GetResponse) decoded).getChain();

    Util.assertChainHas(decodedChain, 1L, 11L, 111L);
  }

  @Test
  public void testMapValueCodec() throws Exception {
    Object subject = new Integer(10);
    EhcacheEntityResponse mapValue = mapValue(subject);
    EhcacheEntityResponse.MapValue decoded =
        (EhcacheEntityResponse.MapValue) RESPONSE_CODEC.decode(RESPONSE_CODEC.encode(mapValue));
    assertThat(decoded.getValue(), equalTo(subject));
  }

  @Test
  public void testSuccess() throws Exception {
    byte[] encoded = RESPONSE_CODEC.encode(success());
    assertThat(RESPONSE_CODEC.decode(encoded), Matchers.<EhcacheEntityResponse>sameInstance(success()));
  }

  @Test
  public void testHashInvalidationDone() throws Exception {
    EhcacheEntityResponse.HashInvalidationDone response = hashInvalidationDone(KEY);
    byte[] encoded = RESPONSE_CODEC.encode(response);
    EhcacheEntityResponse.HashInvalidationDone decodedResponse = (EhcacheEntityResponse.HashInvalidationDone) RESPONSE_CODEC.decode(encoded);

    assertThat(decodedResponse.getResponseType(), is(EhcacheResponseType.HASH_INVALIDATION_DONE));
    assertThat(decodedResponse.getKey(), is(KEY));
  }

  @Test
  public void testAllInvalidationDone() throws Exception {
    EhcacheEntityResponse.AllInvalidationDone response = allInvalidationDone();

    byte[] encoded = RESPONSE_CODEC.encode(response);
    EhcacheEntityResponse.AllInvalidationDone decodedResponse = (EhcacheEntityResponse.AllInvalidationDone) RESPONSE_CODEC.decode(encoded);

    assertThat(decodedResponse.getResponseType(), is(EhcacheResponseType.ALL_INVALIDATION_DONE));
  }

  @Test
  public void testClientInvalidateHash() throws Exception {
    EhcacheEntityResponse.ClientInvalidateHash response = clientInvalidateHash(KEY, INVALIDATION_ID);
    byte[] encoded = RESPONSE_CODEC.encode(response);
    EhcacheEntityResponse.ClientInvalidateHash decodedResponse = (EhcacheEntityResponse.ClientInvalidateHash) RESPONSE_CODEC.decode(encoded);

    assertThat(decodedResponse.getResponseType(), is(EhcacheResponseType.CLIENT_INVALIDATE_HASH));
    assertThat(decodedResponse.getKey(), is(KEY));
    assertThat(decodedResponse.getInvalidationId(), is(INVALIDATION_ID));
  }

  @Test
  public void testClientInvalidateAll() throws Exception {
    EhcacheEntityResponse.ClientInvalidateAll response = clientInvalidateAll(INVALIDATION_ID);

    byte[] encoded = RESPONSE_CODEC.encode(response);
    EhcacheEntityResponse.ClientInvalidateAll decodedResponse = (EhcacheEntityResponse.ClientInvalidateAll) RESPONSE_CODEC.decode(encoded);

    assertThat(decodedResponse.getResponseType(), is(EhcacheResponseType.CLIENT_INVALIDATE_ALL));
    assertThat(decodedResponse.getInvalidationId(), is(INVALIDATION_ID));
  }

  @Test
  public void testServerInvalidateHash() throws Exception {
    EhcacheEntityResponse.ServerInvalidateHash response = serverInvalidateHash(KEY);

    byte[] encoded = RESPONSE_CODEC.encode(response);
    EhcacheEntityResponse.ServerInvalidateHash decodedResponse = (EhcacheEntityResponse.ServerInvalidateHash) RESPONSE_CODEC.decode(encoded);

    assertThat(decodedResponse.getResponseType(), is(EhcacheResponseType.SERVER_INVALIDATE_HASH));
    assertThat(decodedResponse.getKey(), is(KEY));
  }

  @Test
  public void testPrepareForDestroy() throws Exception {
    Set<String> storeIdentifiers = new HashSet<>();
    storeIdentifiers.add("store1");
    storeIdentifiers.add("anotherStore");
    EhcacheEntityResponse.PrepareForDestroy response = prepareForDestroy(storeIdentifiers);

    byte[] encoded = RESPONSE_CODEC.encode(response);
    EhcacheEntityResponse.PrepareForDestroy decodedResponse = (EhcacheEntityResponse.PrepareForDestroy) RESPONSE_CODEC.decode(encoded);

    assertThat(decodedResponse.getResponseType(), is(EhcacheResponseType.PREPARE_FOR_DESTROY));
    assertThat(decodedResponse.getStores(), is(storeIdentifiers));
  }

  @Test
  public void testResolveRequest() throws Exception {
    long hash = 42L;
    EhcacheEntityResponse.ResolveRequest response = new EhcacheEntityResponse.ResolveRequest(hash, getChain(false,
      createPayload(1L), createPayload(11L), createPayload(111L)));

    byte[] encoded = RESPONSE_CODEC.encode(response);
    EhcacheEntityResponse.ResolveRequest decodedResponse = (EhcacheEntityResponse.ResolveRequest) RESPONSE_CODEC.decode(encoded);

    assertThat(decodedResponse.getResponseType(), is(EhcacheResponseType.RESOLVE_REQUEST));
    assertThat(decodedResponse.getKey(), is(42L));
    Util.assertChainHas(decodedResponse.getChain(), 1L, 11L, 111L);
  }

  @Test
  public void testLockResponse() {
    EhcacheEntityResponse.LockSuccess lockSuccess = new EhcacheEntityResponse.LockSuccess(getChain(false, createPayload(1L), createPayload(10L)));

    byte[] sucessEncoded = RESPONSE_CODEC.encode(lockSuccess);
    EhcacheEntityResponse.LockSuccess successDecoded = (EhcacheEntityResponse.LockSuccess) RESPONSE_CODEC.decode(sucessEncoded);

    assertThat(successDecoded.getResponseType(), is(EhcacheResponseType.LOCK_SUCCESS));
    Util.assertChainHas(successDecoded.getChain(), 1L, 10L);

    EhcacheEntityResponse.LockFailure lockFailure = EhcacheEntityResponse.lockFailure();
    byte[] failureEncoded = RESPONSE_CODEC.encode(lockFailure);
    EhcacheEntityResponse.LockFailure failureDecoded = (EhcacheEntityResponse.LockFailure) RESPONSE_CODEC.decode(failureEncoded);

    assertThat(failureDecoded.getResponseType(), is(EhcacheResponseType.LOCK_FAILURE));
  }
}
