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
package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.Util;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 */
public class ChainBuilderTest {

  @Test
  public void testChainBuilder() {
    ChainBuilder cb1 = new ChainBuilder();

    ChainBuilder cb2 = cb1.add(Util.createPayload(1L))
                          .add(Util.createPayload(3L))
                          .add(Util.createPayload(4L));

    ChainBuilder cb3  = cb2.add(Util.createPayload(2L));

    Chain chain1 = cb1.build();
    Chain chain2 = cb2.build();
    Chain chain3 = cb3.build();

    assertChainHas(chain1);
    assertChainHas(chain2, 1L, 3L, 4L);
    assertChainHas(chain3, 1L, 3L, 4L, 2L);

  }

  private static void assertChainHas(Chain chain, long... payLoads) {
    Iterator<Element> elements = chain.iterator();
    for (long payLoad : payLoads) {
      assertThat(Util.readPayLoad(elements.next().getPayload()), is(Long.valueOf(payLoad)));
    }
    assertThat(elements.hasNext(), is(false));
  }
}
