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

package org.ehcache.clustered.util;

import org.ehcache.core.spi.store.Store;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.ValueStatistic;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

/**
 * StatisticsTestUtils
 */
public class StatisticsTestUtils {
  /**
   * Validates expected {@link OperationStatistic} updates for the
   * indicated {@code Ehcache} instance.  The statistics identified in {@code changed} are
   * checked for a value of {@code 1}; all other statistics in the same enumeration class are
   * checked for a value of {@code 0}.
   *
   * @param store the store instance to check
   * @param changed the statistics values that should have updated values
   * @param <E> the statistics enumeration type
   */
  public static <E extends Enum<E>> void validateStats(final Store<?, ?> store, final EnumSet<E> changed) {
    assert changed != null;
    final EnumSet<E> unchanged = EnumSet.complementOf(changed);

    @SuppressWarnings("unchecked")
    final List<EnumSet<E>> sets = Arrays.asList(changed, unchanged);
    Class<E> statsClass = null;
    for (final EnumSet<E> set : sets) {
      if (!set.isEmpty()) {
        statsClass = set.iterator().next().getDeclaringClass();
        break;
      }
    }
    assert statsClass != null;

    final OperationStatistic<E> operationStatistic = getOperationStatistic(store, statsClass);
    for (final E statId : changed) {
      Assert.assertThat(String.format("Value for %s.%s", statId.getDeclaringClass().getName(), statId.name()),
          getStatistic(operationStatistic, statId), StatisticMatcher.equalTo(1L));
    }
    for (final E statId : unchanged) {
      Assert.assertThat(String.format("Value for %s.%s", statId.getDeclaringClass().getName(), statId.name()),
          getStatistic(operationStatistic, statId), StatisticMatcher.equalTo(0L));
    }
  }

  public static <E extends Enum<E>> void validateStat(final Store<?, ?> store, E outcome, long count) {
    OperationStatistic<E> operationStatistic = getOperationStatistic(store, outcome.getDeclaringClass());
    Assert.assertThat(getStatistic(operationStatistic, outcome), StatisticMatcher.equalTo(count));
  }

  /**
   * Gets the value of the statistic indicated from an {@link OperationStatistic}
   * instance.
   *
   * @param operationStatistic the {@code OperationStatistic} instance from which the statistic is to
   *            be obtained
   * @param statId the {@code Enum} constant identifying the statistic for which the value must be obtained
   * @param <E> The {@code Enum} type for the statistics
   *
   * @return the value, possibly null, for {@code statId} about {@code ehcache}
   */
  private static <E extends Enum<E>> Number getStatistic(final OperationStatistic<E> operationStatistic, final E statId) {
    if (operationStatistic != null) {
      final ValueStatistic<Long> valueStatistic = operationStatistic.statistic(statId);
      return (valueStatistic == null ? null : valueStatistic.value());
    }
    return null;
  }

  /**
   * Gets a reference to the {@link OperationStatistic} instance holding the
   * class of statistics specified for the {@code Ehcache} instance provided.
   *
   * @param store the store instance for which the {@code OperationStatistic} instance
   *          should be obtained
   * @param statsClass the {@code Class} of statistics for which the {@code OperationStatistic} instance
   *          should be obtained
   * @param <E> the {@code Enum} type for the statistics
   *
   * @return a reference to the {@code OperationStatistic} instance holding the {@code statsClass} statistics;
   *          may be {@code null} if {@code statsClass} statistics do not exist for {@code ehcache}
   */
  private static <E extends Enum<E>> OperationStatistic<E> getOperationStatistic(final Store<?, ?> store, final Class<E> statsClass) {
    for (final TreeNode statNode : ContextManager.nodeFor(store).getChildren()) {
      final Object statObj = statNode.getContext().attributes().get("this");
      if (statObj instanceof OperationStatistic<?>) {
        @SuppressWarnings("unchecked")
        final OperationStatistic<E> statistic = (OperationStatistic<E>)statObj;
        if (statistic.type().equals(statsClass)) {
          return statistic;
        }
      }
    }
    return null;
  }

  /**
   * Local {@code org.hamcrest.TypeSafeMatcher} implementation for testing
   * {@code org.terracotta.statistics.OperationStatistic} values.
   */
  private static final class StatisticMatcher extends TypeSafeMatcher<Number> {

    final Number expected;

    private StatisticMatcher(final Class<?> expectedType, final Number expected) {
      super(expectedType);
      this.expected = expected;
    }

    @Override
    protected boolean matchesSafely(final Number value) {
      if (value != null) {
        return (value.longValue() == this.expected.longValue());
      } else {
        return this.expected.longValue() == 0L;
      }
    }

    @Override
    public void describeTo(final Description description) {
      if (this.expected.longValue() == 0L) {
        description.appendText("zero or null");
      } else {
        description.appendValue(this.expected);
      }
    }

    @Factory
    public static Matcher<Number> equalTo(final Number expected) {
      return new StatisticMatcher(Number.class, expected);
    }
  }
}
