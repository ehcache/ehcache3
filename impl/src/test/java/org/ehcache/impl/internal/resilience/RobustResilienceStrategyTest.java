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
package org.ehcache.impl.internal.resilience;

import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.StoreAccessException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.Arrays;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.of;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class RobustResilienceStrategyTest {

  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  @Mock
  private RecoveryStore<Integer> store;

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
    verify(store).obliterate(1);
  }

  @Test
  public void containsKeyFailure() throws StoreAccessException {
    assertThat(strategy.containsKeyFailure(1, accessException)).isFalse();
    verify(store).obliterate(1);
  }

  @Test
  public void putFailure() throws StoreAccessException {
    strategy.putFailure(1, 1L, accessException);
    verify(store).obliterate(1);
  }

  @Test
  public void removeFailure() throws StoreAccessException {
    assertThat(strategy.removeFailure(1, 1L, accessException)).isFalse();
    verify(store).obliterate(1);
  }

  @Test
  public void clearFailure() throws StoreAccessException {
    strategy.clearFailure(accessException);
    verify(store).obliterate();
  }

  @Test
  public void putIfAbsentFailure() throws StoreAccessException {
    assertThat(strategy.putIfAbsentFailure(1, 1L, accessException)).isNull();
    verify(store).obliterate(1);
  }

  @Test
  public void removeFailure1() throws StoreAccessException {
    assertThat(strategy.removeFailure(1, 1L, accessException)).isFalse();
    verify(store).obliterate(1);
  }

  @Test
  public void replaceFailure() throws StoreAccessException {
    assertThat(strategy.replaceFailure(1, 1L, accessException)).isNull();
    verify(store).obliterate(1);
  }

  @Test
  public void replaceFailure1() throws StoreAccessException {
    assertThat(strategy.replaceFailure(1, 1L, 2L, accessException)).isFalse();
    verify(store).obliterate(1);
  }

  @Test
  public void getAllFailure() throws StoreAccessException {
    assertThat(strategy.getAllFailure(asList(1, 2), accessException)).containsExactly(entry(1, null), entry(2, null));
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Iterable<Integer>> captor = ArgumentCaptor.forClass(Iterable.class);
    verify(store).obliterate(captor.capture());
    assertThat(captor.getValue()).contains(1, 2);
  }

  @Test
  public void putAllFailure() throws StoreAccessException {
    strategy.putAllFailure(of(1, 2).collect(toMap(identity(), k -> (long) k)), accessException);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Iterable<Integer>> captor = ArgumentCaptor.forClass(Iterable.class);
    verify(store).obliterate(captor.capture());
    assertThat(captor.getValue()).contains(1, 2);
  }

  @Test
  public void removeAllFailure() throws StoreAccessException {
    strategy.removeAllFailure(asList(1, 2), accessException);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Iterable<Integer>> captor = ArgumentCaptor.forClass(Iterable.class);
    verify(store).obliterate(captor.capture());
    assertThat(captor.getValue()).contains(1, 2);
  }
}
