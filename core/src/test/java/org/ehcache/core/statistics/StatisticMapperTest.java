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
package org.ehcache.core.statistics;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.observer.ChainedOperationObserver;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import static java.util.EnumSet.of;
import static org.ehcache.core.statistics.StatisticMapperTest.Source.C;
import static org.ehcache.core.statistics.StatisticMapperTest.Source.D;
import static org.ehcache.core.statistics.StatisticMapperTest.Source.E;
import static org.ehcache.core.statistics.StatisticMapperTest.Target.A;
import static org.ehcache.core.statistics.StatisticMapperTest.Target.B;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class StatisticMapperTest {

  @Test
  public void testInvalidSourceStatisticSet() {
    try {
      new StatisticMapper<Source, Target>(Collections.<Target, Set<Source>>singletonMap(A, of(C, D)), null);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("target outcomes [B]"));
    }
  }

  @Test
  public void testInvalidTargetStatisticSet() {
    try {
      Map<Target, Set<Source>> translation = new EnumMap<Target, Set<Source>>(Target.class);
      translation.put(A, of(C));
      translation.put(B, of(D));
      new StatisticMapper<Source, Target>(translation, null);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("source outcomes [E]"));
    }
  }

  @Test
  public void testIllDefinedTranslation() {
    try {
      Map<Target, Set<Source>> translation = new EnumMap<Target, Set<Source>>(Target.class);
      translation.put(A, of(C, D));
      translation.put(B, of(D, E));
      new StatisticMapper<Source, Target>(translation, null);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("mapping is ill-defined"));
    }
  }

  @Test
  public void testTargetTypeExtraction() {
    Map<Target, Set<Source>> translation = new EnumMap<Target, Set<Source>>(Target.class);
    translation.put(A, of(C));
    translation.put(B, of(D, E));
    StatisticMapper<Source, Target> mapper = new StatisticMapper<Source, Target>(translation, null);

    assertThat(mapper.type(), equalTo(Target.class));
  }

  @Test
  public void testStatisticTranslation() {
    OperationStatistic<Source> statistic = mock(OperationStatistic.class);
    Map<Target, Set<Source>> translation = new EnumMap<Target, Set<Source>>(Target.class);
    translation.put(A, of(C));
    translation.put(B, of(D, E));
    StatisticMapper<Source, Target  > mapper = new StatisticMapper<Source, Target>(translation, statistic);

    mapper.statistic(B);
    verify(statistic).statistic(of(D, E));

    mapper.statistic(A);
    verify(statistic).statistic(of(C));
  }

  @Test
  public void testStatisticSetTranslation() {
    OperationStatistic<Source> statistic = mock(OperationStatistic.class);
    Map<Target, Set<Source>> translation = new EnumMap<Target, Set<Source>>(Target.class);
    translation.put(A, of(C));
    translation.put(B, of(D, E));
    StatisticMapper<Source, Target  > mapper = new StatisticMapper<Source, Target>(translation, statistic);

    mapper.statistic(of(A, B));
    verify(statistic).statistic(of(C, D, E));
  }

  @Test
  public void testCountTranslation() {
    OperationStatistic<Source> statistic = mock(OperationStatistic.class);
    Map<Target, Set<Source>> translation = new EnumMap<Target, Set<Source>>(Target.class);
    translation.put(A, of(C));
    translation.put(B, of(D, E));
    StatisticMapper<Source, Target  > mapper = new StatisticMapper<Source, Target>(translation, statistic);

    mapper.count(B);
    verify(statistic).sum(of(D, E));

    mapper.count(A);
    verify(statistic).sum(of(C));
  }

  @Test
  public void testSumTranslation() {
    OperationStatistic<Source> statistic = mock(OperationStatistic.class);
    Map<Target, Set<Source>> translation = new EnumMap<Target, Set<Source>>(Target.class);
    translation.put(A, of(C));
    translation.put(B, of(D, E));
    StatisticMapper<Source, Target  > mapper = new StatisticMapper<Source, Target>(translation, statistic);

    mapper.sum(of(A, B));
    verify(statistic).sum(of(C, D, E));
  }

  @Test
  public void testFullSum() {
    OperationStatistic<Source> statistic = mock(OperationStatistic.class);
    Map<Target, Set<Source>> translation = new EnumMap<Target, Set<Source>>(Target.class);
    translation.put(A, of(C));
    translation.put(B, of(D, E));
    StatisticMapper<Source, Target  > mapper = new StatisticMapper<Source, Target>(translation, statistic);

    mapper.sum();

    verify(statistic).sum();
  }

  @Test
  public void testDerivedStatisticBeginDelegation() {
    ArgumentCaptor<ChainedOperationObserver> wrapperCapture = ArgumentCaptor.forClass(ChainedOperationObserver.class);

    OperationStatistic<Source> statistic = mock(OperationStatistic.class);
    doNothing().when(statistic).addDerivedStatistic(wrapperCapture.capture());

    Map<Target, Set<Source>> translation = new EnumMap<Target, Set<Source>>(Target.class);
    translation.put(A, of(C));
    translation.put(B, of(D, E));
    StatisticMapper<Source, Target  > mapper = new StatisticMapper<Source, Target>(translation, statistic);

    ChainedOperationObserver<? super Target> derived = mock(ChainedOperationObserver.class);

    mapper.addDerivedStatistic(derived);

    ChainedOperationObserver<Source> wrapper = wrapperCapture.getValue();

    wrapper.begin(42L);
    verify(derived).begin(42L);
  }

  @Test
  public void testDerivedStatisticEndDelegation() {
    ArgumentCaptor<ChainedOperationObserver> wrapperCapture = ArgumentCaptor.forClass(ChainedOperationObserver.class);

    OperationStatistic<Source> statistic = mock(OperationStatistic.class);
    doNothing().when(statistic).addDerivedStatistic(wrapperCapture.capture());

    Map<Target, Set<Source>> translation = new EnumMap<Target, Set<Source>>(Target.class);
    translation.put(A, of(C));
    translation.put(B, of(D, E));
    StatisticMapper<Source, Target  > mapper = new StatisticMapper<Source, Target>(translation, statistic);

    ChainedOperationObserver<? super Target> derived = mock(ChainedOperationObserver.class);

    mapper.addDerivedStatistic(derived);

    ChainedOperationObserver<Source> wrapper = wrapperCapture.getValue();

    wrapper.end(43L, E);
    verify(derived).end(43L, B);

    wrapper.end(44L, C);
    verify(derived).end(44L, A);

    wrapper.end(45L, D);
    verify(derived).end(45L, B);
  }

  @Test
  public void testDerivedStatisticEndWithParametersDelegation() {
    ArgumentCaptor<ChainedOperationObserver> wrapperCapture = ArgumentCaptor.forClass(ChainedOperationObserver.class);

    OperationStatistic<Source> statistic = mock(OperationStatistic.class);
    doNothing().when(statistic).addDerivedStatistic(wrapperCapture.capture());

    Map<Target, Set<Source>> translation = new EnumMap<Target, Set<Source>>(Target.class);
    translation.put(A, of(C));
    translation.put(B, of(D, E));
    StatisticMapper<Source, Target  > mapper = new StatisticMapper<Source, Target>(translation, statistic);

    ChainedOperationObserver<? super Target> derived = mock(ChainedOperationObserver.class);

    mapper.addDerivedStatistic(derived);

    ChainedOperationObserver<Source> wrapper = wrapperCapture.getValue();

    wrapper.end(43L, E, 1L, 2L);
    verify(derived).end(43L, B, 1L, 2L);

    wrapper.end(44L, C, 2L, 1L);
    verify(derived).end(44L, A, 2L, 1L);

    wrapper.end(45L, D, 3L, 4L);
    verify(derived).end(45L, B, 3L, 4L);
  }

  @Test
  public void testDerivedStatisticRemovalDelegation() {
    OperationStatistic<Source> statistic = mock(OperationStatistic.class);

    Map<Target, Set<Source>> translation = new EnumMap<Target, Set<Source>>(Target.class);
    translation.put(A, of(C));
    translation.put(B, of(D, E));
    StatisticMapper<Source, Target  > mapper = new StatisticMapper<Source, Target>(translation, statistic);

    ChainedOperationObserver<? super Target> derived = mock(ChainedOperationObserver.class);

    mapper.addDerivedStatistic(derived);
    mapper.removeDerivedStatistic(derived);

    verify(statistic).removeDerivedStatistic(any(ChainedOperationObserver.class));
  }

  enum Target {
    A, B
  }

  enum Source {
    C, D, E
  }
}
