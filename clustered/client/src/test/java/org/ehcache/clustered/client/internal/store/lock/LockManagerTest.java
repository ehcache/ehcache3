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
package org.ehcache.clustered.client.internal.store.lock;

import org.ehcache.clustered.client.internal.store.ClusterTierClientEntity;
import org.ehcache.clustered.client.internal.store.ServerStoreProxyException;
import org.ehcache.clustered.common.internal.exceptions.UnknownClusterException;
import org.ehcache.clustered.common.internal.messages.ClusterTierReconnectMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.LockSuccess;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage.LockMessage;
import org.ehcache.clustered.common.internal.store.Chain;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.ehcache.clustered.ChainUtils.chainOf;
import static org.ehcache.clustered.ChainUtils.createPayload;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.lockFailure;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.*;

public class LockManagerTest {

  @Test
  public void testLock() throws Exception {
    ClusterTierClientEntity clusterTierClientEntity = mock(ClusterTierClientEntity.class);

    LockManager lockManager = new LockManager(clusterTierClientEntity);

    LockSuccess lockSuccess = getLockSuccessResponse();

    when(clusterTierClientEntity.invokeAndWaitForComplete(any(LockMessage.class), anyBoolean()))
            .thenReturn(lockSuccess);

    Chain lock = lockManager.lock(2L);

    assertThat(lock, notNullValue());
    assertThat(lock.length(), is(3));

  }

  @Test
  public void testLockWhenException() throws Exception {
    ClusterTierClientEntity clusterTierClientEntity = mock(ClusterTierClientEntity.class);

    LockManager lockManager = new LockManager(clusterTierClientEntity);

    when(clusterTierClientEntity.invokeAndWaitForComplete(any(LockMessage.class), anyBoolean()))
            .thenThrow(new UnknownClusterException(""), new TimeoutException("timed out test"));

    try {
      lockManager.lock(2L);
      fail();
    } catch (ServerStoreProxyException sspe) {
      assertThat(sspe.getCause(), instanceOf(UnknownClusterException.class));
    }

    try {
      lockManager.lock(2L);
      fail();
    } catch (TimeoutException e) {
      assertThat(e.getMessage(), is("timed out test"));
    }

  }

  @Test
  public void testLockWhenFailure() throws Exception {
    ClusterTierClientEntity clusterTierClientEntity = mock(ClusterTierClientEntity.class);

    LockManager lockManager = new LockManager(clusterTierClientEntity);

    LockSuccess lockSuccess = getLockSuccessResponse();

    when(clusterTierClientEntity.invokeAndWaitForComplete(any(LockMessage.class), anyBoolean()))
            .thenReturn(lockFailure(), lockFailure(), lockFailure(), lockSuccess);

    Chain lock = lockManager.lock(2L);

    assertThat(lock, notNullValue());
    assertThat(lock.length(), is(3));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testUnlockClearsLocksHeldState() throws Exception {
    ClusterTierClientEntity clusterTierClientEntity = mock(ClusterTierClientEntity.class);
    LockManager lockManager = new LockManager(clusterTierClientEntity);

    LockSuccess lockSuccess = getLockSuccessResponse();
    when(clusterTierClientEntity.invokeAndWaitForComplete(any(LockMessage.class), anyBoolean()))
      .thenReturn(lockSuccess);

    Chain lock = lockManager.lock(2L);
    lockManager.unlock(2L, false);

    ClusterTierReconnectMessage reconnectMessage = mock(ClusterTierReconnectMessage.class);
    ArgumentCaptor<Set<Long>> locks = ArgumentCaptor.forClass(Set.class);
    doNothing().when(reconnectMessage).addLocksHeld(locks.capture());
    lockManager.reconnectListener(reconnectMessage);
    assertThat(locks.getValue().size(), is(0));

  }

  private LockSuccess getLockSuccessResponse() {
    ByteBuffer[] buffers = new ByteBuffer[3];
    for (int i = 0; i < 3; i++) {
      buffers[i] = createPayload(i + 1);
    }

    Chain chain = chainOf(buffers);

    return EhcacheEntityResponse.lockSuccess(chain);
  }

}
