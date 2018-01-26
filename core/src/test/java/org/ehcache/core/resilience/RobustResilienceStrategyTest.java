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
package org.ehcache.core.resilience;

import org.ehcache.core.internal.util.CollectionUtil;
import org.ehcache.core.spi.store.Store;
import org.ehcache.resilience.RethrowingStoreAccessException;
import org.ehcache.resilience.StoreAccessException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class RobustResilienceStrategyTest {

  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  @Mock
  private Store<Integer, Long> store;

  @InjectMocks
  private RobustResilienceStrategy<Integer, Long> strategy;

  private final StoreAccessException accessException = new StoreAccessException("The exception");

  @After
  public void noMoreInteractions() {
    verifyNoMoreInteractions(store);
  }

  @Test
  public void getFailure() throws StoreAccessException {
    assertThat(strategy.getFailure(1, accessException)).isNull();
    verify(store).remove(1);
  }

  @Test
  public void containsKeyFailure() throws StoreAccessException {
    assertThat(strategy.containsKeyFailure(1, accessException)).isFalse();
    verify(store).remove(1);
  }

  @Test
  public void putFailure() throws StoreAccessException {
    strategy.putFailure(1, 1L, accessException);
    verify(store).remove(1);
  }

  @Test
  public void removeFailure() throws StoreAccessException {
    assertThat(strategy.removeFailure(1, 1L, accessException)).isFalse();
    verify(store).remove(1);
  }

  @Test
  public void clearFailure() throws StoreAccessException {
    strategy.clearFailure(accessException);
    verify(store).clear();
  }

  @Test
  public void putIfAbsentFailure() throws StoreAccessException {
    assertThat(strategy.putIfAbsentFailure(1, 1L, accessException)).isNull();
    verify(store).remove(1);
  }

  @Test
  public void removeFailure1() throws StoreAccessException {
    assertThat(strategy.removeFailure(1, 1L, accessException)).isFalse();
    verify(store).remove(1);
  }

  @Test
  public void replaceFailure() throws StoreAccessException {
    assertThat(strategy.replaceFailure(1, 1L, accessException)).isNull();
    verify(store).remove(1);
  }

  @Test
  public void replaceFailure1() throws StoreAccessException {
    assertThat(strategy.replaceFailure(1, 1L, 2L, accessException)).isFalse();
    verify(store).remove(1);
  }

  @Test
  public void getAllFailure() throws StoreAccessException {
    assertThat(strategy.getAllFailure(Arrays.asList(1, 2), accessException)).containsExactly(entry(1, null), entry(2, null));
    verify(store).remove(1);
    verify(store).remove(2);
  }

  @Test
  public void putAllFailure() throws StoreAccessException {
    strategy.putAllFailure(CollectionUtil.map(1, 2L, 2, 2L), accessException);
    verify(store).remove(1);
    verify(store).remove(2);
  }

  @Test
  public void removeAllFailure() throws StoreAccessException {
    strategy.removeAllFailure(Arrays.asList(1, 2), accessException);
    verify(store).remove(1);
    verify(store).remove(2);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void filterException_SAE() {
    // Nothing happens
    strategy.filterException(accessException);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void filterException_RSAE() {
    assertThatExceptionOfType(RuntimeException.class)
      .isThrownBy(() -> strategy.filterException(new RethrowingStoreAccessException(new RuntimeException())));
  }
}
