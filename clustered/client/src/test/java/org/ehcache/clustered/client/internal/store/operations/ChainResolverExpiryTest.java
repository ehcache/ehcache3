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

package org.ehcache.clustered.client.internal.store.operations;

import org.ehcache.ValueSupplier;
import org.ehcache.clustered.client.TestTimeSource;
import org.ehcache.clustered.client.internal.store.ChainBuilder;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.store.Chain;
import org.ehcache.expiry.Expiry;
import org.ehcache.impl.serialization.LongSerializer;
import org.ehcache.impl.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ChainResolverExpiryTest {

  private static final OperationsCodec<Long, String> codec = new OperationsCodec(new LongSerializer(), new StringSerializer());

  private static TestTimeSource timeSource = null;

  @Before
  public void initialSetup() {
    timeSource = new TestTimeSource();
  }

  @Test
  public void testGetExpiryForAccessIsIgnored() {
    Expiry<Long, String> expiry = mock(Expiry.class);
    ChainResolver<Long, String> chainResolver = new ChainResolver(codec, expiry);

    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "One", timeSource.getTimeMillis(), true));
    list.add(new PutOperation<Long, String>(1L, "Second", timeSource.getTimeMillis(), true));

    Chain chain = getChainFromOperations(list);

    chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    verify(expiry, times(0)).getExpiryForAccess(anyLong(), any(ValueSupplier.class));
    verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    verify(expiry, times(1)).getExpiryForUpdate(anyLong(), any(ValueSupplier.class), anyString());

  }

  @Test
  public void testGetExpiryForCreationIsInvokedOnlyOnce() {
    Expiry<Long, String> expiry = mock(Expiry.class);
    ChainResolver<Long, String> chainResolver = new ChainResolver(codec, expiry);

    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "One", timeSource.getTimeMillis(), true));
    list.add(new PutOperation<Long, String>(1L, "Second", timeSource.getTimeMillis(), true));
    list.add(new PutOperation<Long, String>(1L, "Three", timeSource.getTimeMillis(), true));
    list.add(new PutOperation<Long, String>(1L, "Four", timeSource.getTimeMillis(), true));

    Chain chain = getChainFromOperations(list);

    chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    InOrder inOrder = inOrder(expiry);

    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    inOrder.verify(expiry, times(3)).getExpiryForUpdate(anyLong(), any(ValueSupplier.class), anyString());

  }

  @Test
  public void testGetExpiryForCreationIsNotInvokedForReplacedChains() {
    Expiry<Long, String> expiry = mock(Expiry.class);
    ChainResolver<Long, String> chainResolver = new ChainResolver(codec, expiry);

    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Replaced", timeSource.getTimeMillis(), false));
    list.add(new PutOperation<Long, String>(1L, "SecondAfterReplace", timeSource.getTimeMillis(), true));
    list.add(new PutOperation<Long, String>(1L, "ThirdAfterReplace", timeSource.getTimeMillis(), true));
    list.add(new PutOperation<Long, String>(1L, "FourthAfterReplace", timeSource.getTimeMillis(), true));

    Chain chain = getChainFromOperations(list);

    chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());
    verify(expiry, times(0)).getExpiryForCreation(anyLong(), anyString());
    verify(expiry, times(4)).getExpiryForUpdate(anyLong(), any(ValueSupplier.class), anyString());

  }

  @Test
  public void testGetExpiryForCreationIsInvokedAfterRemoveOperations() {

    Expiry<Long, String> expiry = mock(Expiry.class);
    ChainResolver<Long, String> chainResolver = new ChainResolver(codec, expiry);

    List<Operation<Long, String>> list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "Replaced", timeSource.getTimeMillis(), false));
    list.add(new PutOperation<Long, String>(1L, "SecondAfterReplace", timeSource.getTimeMillis(), true));
    list.add(new RemoveOperation<Long, String>(1L, timeSource.getTimeMillis(), true));
    list.add(new PutOperation<Long, String>(1L, "FourthAfterReplace", timeSource.getTimeMillis(), true));

    Chain replacedChain = getChainFromOperations(list);

    chainResolver.resolve(replacedChain, 1L, timeSource.getTimeMillis());

    InOrder inOrder = inOrder(expiry);

    verify(expiry, times(0)).getExpiryForAccess(anyLong(), any(ValueSupplier.class));
    inOrder.verify(expiry, times(2)).getExpiryForUpdate(anyLong(), any(ValueSupplier.class), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());

    reset(expiry);

    list = new ArrayList<Operation<Long, String>>();
    list.add(new PutOperation<Long, String>(1L, "One", timeSource.getTimeMillis(), true));
    list.add(new PutOperation<Long, String>(1L, "Second", timeSource.getTimeMillis(), true));
    list.add(new RemoveOperation<Long, String>(1L, timeSource.getTimeMillis(), true));
    list.add(new PutOperation<Long, String>(1L, "Four", timeSource.getTimeMillis(), true));

    Chain chain = getChainFromOperations(list);

    chainResolver.resolve(chain, 1L, timeSource.getTimeMillis());

    inOrder = inOrder(expiry);

    verify(expiry, times(0)).getExpiryForAccess(anyLong(), any(ValueSupplier.class));
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForUpdate(anyLong(), any(ValueSupplier.class), anyString());
    inOrder.verify(expiry, times(1)).getExpiryForCreation(anyLong(), anyString());

  }

  private Chain getChainFromOperations(List<Operation<Long, String>> operations) {
    ChainBuilder chainBuilder = new ChainBuilder();
    for(Operation<Long, String> operation: operations) {
      chainBuilder = chainBuilder.add(codec.encode(operation));
    }
    return chainBuilder.build();
  }

}
