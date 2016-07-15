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


import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.ehcache.clustered.common.internal.store.Util.createPayload;
import static org.ehcache.clustered.common.internal.store.Util.getChain;

public class EhcacheEntityResponseFactoryTest {

  private static final EhcacheEntityResponseFactory RESPONSE_FACTORY = new EhcacheEntityResponseFactory();

  @Test
  public void testFailureResponse() {
    EhcacheEntityResponse failure = RESPONSE_FACTORY.failure(new IllegalMessageException("Test Exception"));

    assertThat(((EhcacheEntityResponse.Failure)failure).getCause().getMessage(), is("Test Exception"));
  }

  @Test
  public void testGetResponse() {
    EhcacheEntityResponse getResponse = RESPONSE_FACTORY.response(getChain(false,
        createPayload(1L), createPayload(11L), createPayload(111L)));

    Chain chain = ((EhcacheEntityResponse.GetResponse) getResponse).getChain();

    Util.assertChainHas(chain, 1L, 11L, 111L);
  }
}
