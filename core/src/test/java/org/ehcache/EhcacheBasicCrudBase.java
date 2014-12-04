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
package org.ehcache;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.resilience.ResilienceStrategy;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.writer.CacheWriter;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.ValueStatistic;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;

/**
 * Provides testing of basic CRUD operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public abstract class EhcacheBasicCrudBase {

  protected static final CacheConfiguration<String, String> CACHE_CONFIGURATION =
      CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(String.class, String.class);

  @Mock
  protected Store<String, String> store;

  /**
   * Holds a {@link org.mockito.Mockito#spy(Object)}-wrapped reference to the
   * {@link org.ehcache.resilience.ResilienceStrategy ResilienceStrategy} used in the
   * {@link org.ehcache.Ehcache Ehcache} instance being tested.
   *
   * @see #setResilienceStrategySpy(Ehcache) 
   */
  protected ResilienceStrategy<String, String> spiedResilienceStrategy;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }

  /**
   * Validates expected {@link org.terracotta.statistics.OperationStatistic} updates for the
   * indicated {@code Ehcache} instance.  The statistics identified in {@code changed} are
   * checked for a value of {@code 1}; all other statistics in the same enumeration class are
   * checked for a value of {@code 0}.
   *
   * @param ehcache the {@code Ehcache} instance to check
   * @param changed the statistics values that should have updated values
   * @param <E> the statistics enumeration type
   */
  protected static <E extends Enum<E>> void validateStats(final Ehcache<?, ?> ehcache, final EnumSet<E> changed) {
    assert changed != null;
    final EnumSet<E> unchanged = EnumSet.complementOf(changed);

    @SuppressWarnings("unchecked")
    final List<EnumSet<E>> sets = Arrays.asList(changed, unchanged);
    Class<E> statsClass = null;
    for (EnumSet<E> set : sets) {
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
   * class of statistics specified for the {@code Ehcache} instance provided.
   *
   * @param ehcache the {@code Ehcache} instance for which the {@code OperationStatistic} instance
   *          should be obtained
   * @param statsClass the {@code Class} of statistics for which the {@code OperationStatistic} instance
   *          should be obtained
   * @param <E> the {@code Enum} type for the statistics
   *
   * @return a reference to the {@code OperationStatistic} instance holding the {@code statsClass} statistics;
   *          may be {@code null} if {@code statsClass} statistics do not exist for {@code ehcache}
   */
  private static <E extends Enum<E>> OperationStatistic<E> getOperationStatistic(final Ehcache<?, ?> ehcache, final Class<E> statsClass) {
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
   * Returns a Mockito {@code any} Matcher for {@link org.ehcache.function.Function}.
   *
   * @return a Mockito {@code any} matcher for {@code Function}.
   */
  @SuppressWarnings("unchecked")
  protected static Function<? super String, ? extends String> getAnyFunction() {
    return any(Function.class);   // unchecked
  }

  /**
   * Returns a Mockito {@code any} Matcher for {@link org.ehcache.function.BiFunction}.
   *
   * @return a Mockito {@code any} matcher for {@code BiFunction}.
   */
  @SuppressWarnings("unchecked")
  protected static <V extends String> BiFunction<? super String, V, V> getAnyBiFunction() {
    return any(BiFunction.class);   // unchecked
  }

  /**
   * Replaces the {@link org.ehcache.resilience.ResilienceStrategy ResilienceStrategy} instance in the
   * {@link org.ehcache.Ehcache Ehcache} instance provided with a
   * {@link org.mockito.Mockito#spy(Object) Mockito <code>spy</code>} wrapping the original
   * {@code ResilienceStrategy} instance.
   *
   * @param ehcache the {@code Ehcache} instance to alter
   *
   * @return the <code>spy</code>-wrapped {@code ResilienceStrategy} instance
   */
  protected final <K, V> ResilienceStrategy<K, V> setResilienceStrategySpy(final Ehcache<K, V> ehcache) {
    assert ehcache != null;
    try {
      final Field resilienceStrategyField = ehcache.getClass().getDeclaredField("resilienceStrategy");
      resilienceStrategyField.setAccessible(true);
      @SuppressWarnings("unchecked")
      ResilienceStrategy<K, V> resilienceStrategy = (ResilienceStrategy<K, V>)resilienceStrategyField.get(ehcache);
      if (resilienceStrategy != null) {
        resilienceStrategy = spy(resilienceStrategy);
        resilienceStrategyField.set(ehcache, resilienceStrategy);
      }
      return resilienceStrategy;
    } catch (Exception e) {
      throw new AssertionError(String.format("Unable to wrap ResilienceStrategy in Ehcache instance: %s", e));
    }
  }

  /**
   * Provides a basic {@link org.ehcache.spi.cache.Store} implementation for testing.
   * The contract implemented by this {@code Store} is not strictly conformant but
   * should be sufficient for {@code Ehcache} implementation testing.
   */
  // TODO: Use a validated Store implementation.
  protected static class FakeStore implements Store<String, String> {

    private final Map<String, FakeValueHolder> entries;

    public FakeStore(final Map<String, String> entries) {
      // Use of ConcurrentHashMap is required to avoid ConcurrentModificationExceptions using Iterator.remove
      this.entries = new ConcurrentHashMap<String, FakeValueHolder>();
      if (entries != null) {
        for (final Map.Entry<String, String> entry : entries.entrySet()) {
          this.entries.put(entry.getKey(), new FakeValueHolder(entry.getValue()));
        }
      }
    }

    /**
     * Gets a mapping of the entries in this {@code Store}.
     *
     * @return a new, unmodifiable map of the entries in this {@code Store}.
     */
    protected Map<String, String> getEntryMap() {
      final Map<String, String> result = new HashMap<String, String>();
      for (final Map.Entry<String, FakeValueHolder> entry : this.entries.entrySet()) {
        result.put(entry.getKey(), entry.getValue().value());
      }
      return Collections.unmodifiableMap(result);
    }

    @Override
    public ValueHolder<String> get(final String key) throws CacheAccessException {
      final FakeValueHolder valueHolder = this.entries.get(key);
      if (valueHolder != null) {
        valueHolder.lastAccessTime = System.currentTimeMillis();
      }
      return valueHolder;
    }

    @Override
    public boolean containsKey(final String key) throws CacheAccessException {
      return this.entries.containsKey(key);
    }

    @Override
    public void put(final String key, final String value) throws CacheAccessException {
      this.entries.put(key, new FakeValueHolder(value));
    }

    @Override
    public ValueHolder<String> putIfAbsent(final String key, final String value) throws CacheAccessException {
      final FakeValueHolder currentValue = this.entries.get(key);
      if (currentValue == null) {
        this.entries.put(key, new FakeValueHolder(value));
        return null;
      }
      currentValue.lastAccessTime = System.currentTimeMillis();
      return currentValue;
    }

    @Override
    public void remove(final String key) throws CacheAccessException {
      this.entries.remove(key);
    }

    @Override
    public boolean remove(final String key, final String value) throws CacheAccessException {
      final ValueHolder<String> currentValue = this.entries.get(key);
      if (currentValue == null || !currentValue.value().equals(value)) {
        return false;
      }
      this.entries.remove(key);
      return true;
    }

    @Override
    public ValueHolder<String> replace(final String key, final String value) throws CacheAccessException {
      final ValueHolder<String> currentValue = this.entries.get(key);
      if (currentValue != null) {
        this.entries.put(key, new FakeValueHolder(value));
      }
      return currentValue;
    }

    @Override
    public boolean replace(final String key, final String oldValue, final String newValue) throws CacheAccessException {
      final ValueHolder<String> currentValue = this.entries.get(key);
      if (currentValue != null && currentValue.value().equals(oldValue)) {
        this.entries.put(key, new FakeValueHolder(newValue));
        return true;
      }
      return false;
    }

    @Override
    public void clear() throws CacheAccessException {
      this.entries.clear();
    }

    @Override
    public void destroy() throws CacheAccessException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void create() throws CacheAccessException {
    }

    @Override
    public void close() {
      this.entries.clear();
    }

    @Override
    public void init() {
    }

    @Override
    public void maintenance() {
    }

    @Override
    public void enableStoreEventNotifications(final StoreEventListener<String, String> listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void disableStoreEventNotifications() {
      throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * The {@code Iterator} returned by this method <b>does not</b> have a {@code remove}
     * method.  The {@code Iterator} returned by {@code FakeStore.this.entries.entrySet().iterator()}
     * must not throw {@link java.util.ConcurrentModificationException ConcurrentModification}.
     */
    @Override
    public Iterator<Cache.Entry<String, ValueHolder<String>>> iterator() throws CacheAccessException {

      return new Iterator<Cache.Entry<String, ValueHolder<String>>>() {

        final java.util.Iterator<Map.Entry<String, FakeValueHolder>> iterator =
            FakeStore.this.entries.entrySet().iterator();

        @Override
        public boolean hasNext() throws CacheAccessException {
          return this.iterator.hasNext();
        }

        @Override
        public Cache.Entry<String, ValueHolder<String>> next() throws CacheAccessException {

          final Map.Entry<String, FakeValueHolder> cacheEntry = this.iterator.next();
          cacheEntry.getValue().lastAccessTime = System.currentTimeMillis();

          return new Cache.Entry<String, ValueHolder<String>>() {

            @Override
            public String getKey() {
              return cacheEntry.getKey();
            }

            @Override
            public ValueHolder<String> getValue() {
              return cacheEntry.getValue();
            }

            @Override
            public long getCreationTime(final TimeUnit unit) {
              return cacheEntry.getValue().creationTime(unit);
            }

            @Override
            public long getLastAccessTime(final TimeUnit unit) {
              return cacheEntry.getValue().lastAccessTime(unit);
            }

            @Override
            public float getHitRate(final TimeUnit unit) {
              return cacheEntry.getValue().hitRate(unit);
            }
          };
        }
      };
    }

    @Override
    public ValueHolder<String> compute(final String key, final BiFunction<? super String, ? super String, ? extends String> mappingFunction)
        throws CacheAccessException {
      final ValueHolder<String> currentValue = this.entries.get(key);
      final String newValue = mappingFunction.apply(key, (currentValue == null ? null : currentValue.value()));
      if (newValue == null) {
        this.entries.remove(key);
        return null;
      }
      final FakeValueHolder newValueHolder = new FakeValueHolder(newValue);
      this.entries.put(key, newValueHolder);
      return newValueHolder;
    }

    @Override
    public ValueHolder<String> computeIfAbsent(final String key, final Function<? super String, ? extends String> mappingFunction)
        throws CacheAccessException {
      FakeValueHolder currentValue = this.entries.get(key);
      if (currentValue == null) {
        final String newValue = mappingFunction.apply(key);
        if (newValue != null) {
          final FakeValueHolder newValueHolder = new FakeValueHolder(newValue);
          this.entries.put(key, newValueHolder);
          currentValue = newValueHolder;
        }
      } else {
        currentValue.lastAccessTime = System.currentTimeMillis();
      }
      return currentValue;
    }

    @Override
    public ValueHolder<String> computeIfPresent(final String key, final BiFunction<? super String, ? super String, ? extends String> remappingFunction)
        throws CacheAccessException {
      final ValueHolder<String> currentValue = this.entries.get(key);
      if (currentValue != null) {
        final String newValue = remappingFunction.apply(key, currentValue.value());
        if (newValue != null) {
          final FakeValueHolder newValueHolder = new FakeValueHolder(newValue);
          this.entries.put(key, newValueHolder);
          return newValueHolder;
        } else {
          this.entries.remove(key);
        }
      }
      return null;
    }

    @Override
    public Map<String, ValueHolder<String>> bulkCompute(final Set<? extends String> keys, final Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> remappingFunction)
        throws CacheAccessException {
      throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation is based, in part, on the implementation found in
     * {@code org.ehcache.internal.store.OnHeapStore}.  This implementation calls
     * {@code mappingFunction} for each key through an internal function supplied
     * to {@link #computeIfAbsent(String, org.ehcache.function.Function) computeIfAbsent}.
     */
    @Override
    public Map<String, ValueHolder<String>> bulkComputeIfAbsent(final Set<? extends String> keys, final Function<Iterable<? extends String>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> mappingFunction)
        throws CacheAccessException {
      final Map<String, ValueHolder<String>> resultMap = new LinkedHashMap<String, ValueHolder<String>>();
      for (final String key : keys) {
        final ValueHolder<String> newValue = this.computeIfAbsent(key, new Function<String, String>() {
          @Override
          public String apply(final String key) {
            final Map.Entry<? extends String, ? extends String> entry =
                mappingFunction.apply(Collections.singleton(key)).iterator().next();
            return entry.getValue();
          }
        });
        resultMap.put(key, newValue);
      }
      return resultMap;
    }

    /**
     * A {@link org.ehcache.spi.cache.Store.ValueHolder} implementation for use within
     * {@link org.ehcache.EhcacheBasicCrudBase.FakeStore}.
     */
    private static class FakeValueHolder implements ValueHolder<String>  {

      private final String value;
      private final long creationTime;
      private long lastAccessTime;

      public FakeValueHolder(final String value) {
        this.value = value;
        this.creationTime = System.currentTimeMillis();
      }

      @Override
      public String value() {
        return this.value;
      }

      @Override
      public long creationTime(final TimeUnit unit) {
        return unit.convert(this.creationTime, TimeUnit.MICROSECONDS);
      }

      @Override
      public long lastAccessTime(final TimeUnit unit) {
        return unit.convert(this.lastAccessTime, TimeUnit.MICROSECONDS);
      }

      @Override
      public float hitRate(final TimeUnit unit) {
        return 0;
      }
    }
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

  /**
   * Provides a basic {@link org.ehcache.spi.writer.CacheWriter} implementation for
   * testing.  The contract implemented by this {@code CacheWriter} may not be strictly
   * conformant but should be sufficient for {@code Ehcache} implementation testing.
   */
  protected static class FakeCacheWriter implements CacheWriter<String, String> {

    private final Map<String, String> cache = new HashMap<String, String>();

    public FakeCacheWriter(final Map<String, String> entries) {
      if (entries != null) {
        this.cache.putAll(entries);
      }
    }

    Map<String, String> getEntryMap() {
      return Collections.unmodifiableMap(this.cache);
    }

    @Override
    public void write(final String key, final String value) throws Exception {
      this.cache.put(key, value);
    }

    @Override
    public void writeAll(final Iterable<? extends Map.Entry<? extends String, ? extends String>> entries)
        throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public void delete(final String key) throws Exception {
      this.cache.remove(key);
    }

    @Override
    public void deleteAll(final Iterable<? extends String> keys) throws Exception {
      throw new UnsupportedOperationException();
    }
  }
}