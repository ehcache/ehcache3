/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

package org.ehcache.core;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.util.TestCacheConfig;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.ValueStatistic;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;

/**
 * Provides testing of basic CRUD operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
@RunWith(MockitoJUnitRunner.class)
public abstract class EhcacheBasicCrudBase {

  protected static final CacheConfiguration<String, String> CACHE_CONFIGURATION = new TestCacheConfig<>(String.class, String.class);

  @Mock
  protected Store<String, String> store;
  @Mock
  protected CacheEventDispatcher<String, String> cacheEventDispatcher;
  @Mock
  protected ResilienceStrategy<String, String> resilienceStrategy;

  /**
   * Validates expected {@link org.terracotta.statistics.OperationStatistic} updates for the
   * indicated {@code InternalCache} instance.  The statistics identified in {@code changed} are
   * checked for a value of {@code 1}; all other statistics in the same enumeration class are
   * checked for a value of {@code 0}.
   *
   * @param ehcache the {@code InternalCache} instance to check
   * @param changed the statistics values that should have updated values
   * @param <E> the statistics enumeration type
   */
  protected static <E extends Enum<E>> void validateStats(final InternalCache<?, ?> ehcache, final EnumSet<E> changed) {
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

    final OperationStatistic<E> operationStatistic = getOperationStatistic(ehcache, statsClass);
    for (final E statId : changed) {
      assertThat(String.format("Value for %s.%s", statId.getDeclaringClass().getName(), statId.name()),
          getStatistic(operationStatistic, statId), StatisticMatcher.equalTo(1L));
    }
    for (final E statId : unchanged) {
      assertThat(String.format("Value for %s.%s", statId.getDeclaringClass().getName(), statId.name()),
          getStatistic(operationStatistic, statId), StatisticMatcher.equalTo(0L));
    }
  }

  /**
   * Gets a reference to the {@link org.terracotta.statistics.OperationStatistic} instance holding the
   * class of statistics specified for the {@code InternalCache} instance provided.
   *
   * @param ehcache the {@code InternalCache} instance for which the {@code OperationStatistic} instance
   *          should be obtained
   * @param statsClass the {@code Class} of statistics for which the {@code OperationStatistic} instance
   *          should be obtained
   * @param <E> the {@code Enum} type for the statistics
   *
   * @return a reference to the {@code OperationStatistic} instance holding the {@code statsClass} statistics;
   *          may be {@code null} if {@code statsClass} statistics do not exist for {@code ehcache}
   */
  private static <E extends Enum<E>> OperationStatistic<E> getOperationStatistic(final InternalCache<?, ?> ehcache, final Class<E> statsClass) {
    for (final TreeNode statNode : ContextManager.nodeFor(ehcache).getChildren()) {
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
   * Gets the value of the statistic indicated from an {@link org.terracotta.statistics.OperationStatistic}
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
   * Returns a Mockito {@code any} Matcher for {@link Function}.
   *
   * @return a Mockito {@code any} matcher for {@code Function}.
   */
  @SuppressWarnings("unchecked")
  protected static Function<? super String, ? extends String> getAnyFunction() {
    return any(Function.class);   // unchecked
  }

  /**
   * Returns a Mockito {@code any} Matcher for {@link BiFunction}.
   *
   * @return a Mockito {@code any} matcher for {@code BiFunction}.
   */
  @SuppressWarnings("unchecked")
  protected static BiFunction<? super String, String, String> getAnyBiFunction() {
    return any(BiFunction.class);   // unchecked
  }

  /**
   * Returns a Mockito {@code any} Matcher for {@link Supplier Supplier<Boolean>}.
   *
   * @return a Mockito {@code any} matcher for {@code NullaryFunction}.
   */
  @SuppressWarnings("unchecked")
  protected static Supplier<Boolean> getBooleanSupplier() {
    return any(Supplier.class);    // unchecked
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

    public static Matcher<Number> equalTo(final Number expected) {
      return new StatisticMatcher(Number.class, expected);
    }
  }

  /**
   * Provides a basic {@link CacheLoaderWriter} implementation for
   * testing.  The contract implemented by this {@code CacheLoaderWriter} may not be strictly
   * conformant but should be sufficient for {@code Ehcache} implementation testing.
   */
  protected static class FakeCacheLoaderWriter implements CacheLoaderWriter<String, String> {

    private final Map<String, String> entries = new HashMap<>();

    /**
     * Keys for which access results in a thrown {@code Exception}.  This set may be empty.
     */
    private final Set<String> failingKeys;

    private boolean isBulkCacheLoadingExceptionEnabled = false;

    /**
     * The entry key causing the {@link #writeAll(Iterable)} and {@link #deleteAll(Iterable)}
     * methods to fail by throwing an exception <i>other</i> than a
     * {@link BulkCacheWritingException BulkCacheWritingException}.
     *
     * @see #setCompleteFailureKey
     */
    private volatile String completeFailureKey = null;

    public FakeCacheLoaderWriter(final Map<String, String> entries) {
      this(entries, Collections.<String>emptySet());
    }

    public FakeCacheLoaderWriter(final Map<String, String> entries, final Set<String> failingKeys) {
      if (entries != null) {
        this.entries.putAll(entries);
      }

      this.failingKeys = (failingKeys.isEmpty()
          ? Collections.emptySet()
          : Collections.unmodifiableSet(new HashSet<>(failingKeys)));
    }

    public FakeCacheLoaderWriter(final Map<String, String> entries, final Set<String> failingKeys, boolean isBulkCacheLoadingExceptionEnabled) {
      this(entries, failingKeys);
      this.isBulkCacheLoadingExceptionEnabled = isBulkCacheLoadingExceptionEnabled;
    }

    Map<String, String> getEntryMap() {
      return Collections.unmodifiableMap(this.entries);
    }

    /**
     * Sets the key causing the {@link #writeAll(Iterable)} and {@link #deleteAll(Iterable)}
     * methods to throw an exception <i>other</i> that a
     * {@link BulkCacheWritingException BulkCacheWritingException}.
     * <p>
     * If a complete failure is recognized, the cache image maintained by this instance
     * is in an inconsistent state.
     *
     * @param completeFailureKey the key, which when processed by {@code writeAll} or
     *      {@code deleteAll}, causes a complete failure of the method
     */
    final void setCompleteFailureKey(final String completeFailureKey) {
      this.completeFailureKey = completeFailureKey;
    }

    @Override
    public void write(final String key, final String value) throws Exception {
      this.checkFailingKey(key);
      this.entries.put(key, value);
    }

    /**
     * {@inheritDoc}
     * <p>
     * If this method throws an exception <i>other</i> than a
     * {@link BulkCacheWritingException BulkCacheWritingException}, the
     * cache image maintained by this {@code CacheLoaderWriter} is in an inconsistent state.
     */
    @Override
    public void writeAll(final Iterable<? extends Map.Entry<? extends String, ? extends String>> entries)
        throws Exception {

      final Set<String> successes = new LinkedHashSet<>();
      final Map<String, Exception> failures = new LinkedHashMap<>();

      for (final Entry<? extends String, ? extends String> entry : entries) {
        final String key = entry.getKey();
        if (key.equals(this.completeFailureKey)) {
          throw new CompleteFailureException();
        }
        try {
          this.write(key, entry.getValue());
          successes.add(key);
        } catch (Exception e) {
          //noinspection ThrowableResultOfMethodCallIgnored
          failures.put(key, e);
        }
      }

      if (!failures.isEmpty()) {
        throw new BulkCacheWritingException(failures, successes);
      }
    }

    @Override
    public void delete(final String key) throws Exception {
      this.checkFailingKey(key);
      this.entries.remove(key);
    }

    /**
     * {@inheritDoc}
     * <p>
     * If this method throws an exception <i>other</i> than a
     * {@link BulkCacheWritingException BulkCacheWritingException}, the
     * cache image maintained by this {@code CacheLoaderWriter} is in an inconsistent state.
     */
    @Override
    public void deleteAll(final Iterable<? extends String> keys) throws Exception {
      final Set<String> successes = new LinkedHashSet<>();
      final Map<String, Exception> failures = new LinkedHashMap<>();

      for (final String key : keys) {
        if (key.equals(this.completeFailureKey)) {
          throw new CompleteFailureException();
        }
        try {
          this.delete(key);
          successes.add(key);
        } catch (Exception e) {
          //noinspection ThrowableResultOfMethodCallIgnored
          failures.put(key, e);
        }
      }

      if (!failures.isEmpty()) {
        throw new BulkCacheWritingException(failures, successes);
      }
    }

    private void checkFailingKey(final String key) throws FailedKeyException {
      if (this.failingKeys.contains(key)) {
        throw new FailedKeyException(String.format("Accessing failing key: %s", key));
      }
    }

    private static final class CompleteFailureException extends Exception {
      private static final long serialVersionUID = -8796858843677614631L;
      public CompleteFailureException() {
      }
    }

    @Override
    public String load(final String key) throws Exception {
      if (this.failingKeys.contains(key)) {
        throw new FailedKeyException(key);
      }
      return this.entries.get(key);
    }

    @Override
    public Map<String, String> loadAll(final Iterable<? extends String> keys) throws Exception {
      if (isBulkCacheLoadingExceptionEnabled) {
        Map<String, Exception> failures = new HashMap<>();
        Map<String, String> loadedKeys = new HashMap<>();

        Exception loadingException = new RuntimeException("Exception loading keys");

        for (String key : keys) {
          if (failingKeys.contains(key)) {
            failures.put(key, loadingException);
          } else {
            loadedKeys.put(key, this.entries.get(key));
          }
        }
        throw new BulkCacheLoadingException(failures, loadedKeys);
      }

      final Map<String, String> resultMap = new HashMap<>();
      for (final String key : keys) {
        if (this.failingKeys.contains(key)) {
          throw new FailedKeyException(key);
        }
        resultMap.put(key, this.entries.get(key));
      }
      return resultMap;
    }

    private static final class FailedKeyException extends Exception {
      private static final long serialVersionUID = 1085055801147786691L;

      public FailedKeyException(final String message) {
        super(message);
      }
    }
  }
}
