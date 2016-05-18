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
package org.ehcache.clustered.common.messages;

import org.ehcache.clustered.common.store.Chain;
import org.junit.Test;


import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;

public class ResponseCodecTest {

  @Test
  public void testFailureResponseCodec() {
    EhcacheEntityResponse failure = EhcacheEntityResponse.failure(new Exception("Test Exception"));

    EhcacheEntityResponse decoded = ResponseCodec.decode(ResponseCodec.encode(failure));

    assertThat(((EhcacheEntityResponse.Failure)decoded).getCause().getMessage(), is("Test Exception"));
  }

  @Test
  public void testGetResponseCodec() {
    EhcacheEntityResponse getResponse = EhcacheEntityResponse.response(Util.getChain(false,
        Util.createPayload(1L), Util.createPayload(11L), Util.createPayload(111L)));

    EhcacheEntityResponse decoded = ResponseCodec.decode(ResponseCodec.encode(getResponse));

    Chain decodedChain = ((EhcacheEntityResponse.GetResponse) decoded).getChain();

    Util.assertChainHas(decodedChain, 1L, 11L, 111L);
  }
}
