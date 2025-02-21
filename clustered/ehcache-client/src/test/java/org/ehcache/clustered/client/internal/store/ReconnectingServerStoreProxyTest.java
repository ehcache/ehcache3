/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.terracotta.exception.ConnectionClosedException;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;

public class ReconnectingServerStoreProxyTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  ServerStoreProxy proxy;

  @Mock
  Runnable runnable;

  private final ServerStoreProxyException storeProxyException = new ServerStoreProxyException(new ConnectionClosedException("Connection Closed"));

  @InjectMocks
  ReconnectingServerStoreProxy serverStoreProxy;

  @Test
  public void testAppend() throws Exception {
    doThrow(storeProxyException).when(proxy).append(anyLong(), any(ByteBuffer.class));

    assertThrows(ReconnectInProgressException.class, () -> serverStoreProxy.append(0, ByteBuffer.allocate(2)));
  }

  @Test
  public void testGetAndAppend() throws Exception {
    doThrow(storeProxyException).when(proxy).getAndAppend(anyLong(), any(ByteBuffer.class));

    assertThrows(ReconnectInProgressException.class, () -> serverStoreProxy.getAndAppend(0, ByteBuffer.allocate(2)));
  }

  @Test
  public void testGet() throws Exception {

    doThrow(storeProxyException).when(proxy).get(anyLong());

    assertThrows(ReconnectInProgressException.class, () -> serverStoreProxy.get(0));
  }

  @Test
  public void testIterator() throws Exception {
    doThrow(storeProxyException).when(proxy).iterator();

    assertThrows(ReconnectInProgressException.class, () -> serverStoreProxy.iterator());
  }
}
