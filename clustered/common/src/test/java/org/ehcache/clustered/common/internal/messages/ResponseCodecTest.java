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
import org.junit.Test;

import java.util.Date;

import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class ResponseCodecTest {

  private static final EhcacheEntityResponseFactory RESPONSE_FACTORY = new EhcacheEntityResponseFactory();
  private static final ResponseCodec RESPONSE_CODEC = new ResponseCodec();

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
}
