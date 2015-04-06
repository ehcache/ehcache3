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
import org.ehcache.exceptions.BulkCacheLoadingException;
import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.resilience.ResilienceStrategy;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
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
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
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
  protected static BiFunction<? super String, String, String> getAnyBiFunction() {
    return any(BiFunction.class);   // unchecked
  }

  /**
   * Returns a Mockito {@code any} Matcher for {@link NullaryFunction NullaryFunction<Boolean>}.
   *
   * @return a Mockito {@code any} matcher for {@code NullaryFunction}.
   */
  @SuppressWarnings("unchecked")
  protected static NullaryFunction<Boolean> getBooleanNullaryFunction() {
    return any(NullaryFunction.class);    // unchecked
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

     private static final NullaryFunction<Boolean> REPLACE_EQUAL_TRUE = new NullaryFunction<Boolean>() {
      @Override
      public Boolean apply() {
        return true;
      }
    };

    /**
     * The key:value pairs served by this {@code Store}.  This map may be empty.
     */
    private final Map<String, FakeValueHolder> entries;

    /**
     * Keys for which access results in a thrown {@code Exception}.  This set may be empty.
     */
    private final Set<String> failingKeys;

    public FakeStore(final Map<String, String> entries) {
      this(entries, Collections.<String>emptySet());
    }

    public FakeStore(final Map<String, String> entries, final Set<String> failingKeys) {
      assert failingKeys != null;

      // Use of ConcurrentHashMap is required to avoid ConcurrentModificationExceptions using Iterator.remove
      this.entries = new ConcurrentHashMap<String, FakeValueHolder>();
      if (entries != null) {
        for (final Map.Entry<String, String> entry : entries.entrySet()) {
          this.entries.put(entry.getKey(), new FakeValueHolder(entry.getValue()));
        }
      }
      this.failingKeys = Collections.unmodifiableSet(new HashSet<String>(failingKeys));
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
      this.checkFailingKey(key);
      final FakeValueHolder valueHolder = this.entries.get(key);
      if (valueHolder != null) {
        valueHolder.lastAccessTime = System.currentTimeMillis();
      }
      return valueHolder;
    }

    @Override
    public boolean containsKey(final String key) throws CacheAccessException {
      this.checkFailingKey(key);
      return this.entries.containsKey(key);
    }

    @Override
    public void put(final String key, final String value) throws CacheAccessException {
      this.checkFailingKey(key);
      this.entries.put(key, new FakeValueHolder(value));
    }

    @Override
    public ValueHolder<String> putIfAbsent(final String key, final String value) throws CacheAccessException {
      this.checkFailingKey(key);
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
      this.checkFailingKey(key);
      this.entries.remove(key);
    }

    @Override
    public boolean remove(final String key, final String value) throws CacheAccessException {
      this.checkFailingKey(key);
      final ValueHolder<String> currentValue = this.entries.get(key);
      if (currentValue == null || !currentValue.value().equals(value)) {
        return false;
      }
      this.entries.remove(key);
      return true;
    }

    @Override
    public ValueHolder<String> replace(final String key, final String value) throws CacheAccessException {
      this.checkFailingKey(key);
      final ValueHolder<String> currentValue = this.entries.get(key);
      if (currentValue != null) {
        this.entries.put(key, new FakeValueHolder(value));
      }
      return currentValue;
    }

    @Override
    public boolean replace(final String key, final String oldValue, final String newValue) throws CacheAccessException {
      this.checkFailingKey(key);
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
          FakeStore.this.checkFailingKey(cacheEntry.getKey());
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

    /**
     * {@inheritDoc}
     * <p/>
     * This method is implemented as
     * <code>this.{@link #compute(String, BiFunction, NullaryFunction) compute}(keys, mappingFunction, () -> { returns true; })</code>
     */
    @Override
    public ValueHolder<String> compute(final String key, final BiFunction<? super String, ? super String, ? extends String> mappingFunction)
        throws CacheAccessException {
      return this.compute(key, mappingFunction, REPLACE_EQUAL_TRUE);
    }

    /**
     * Common core for the
     * {@link #compute(String, BiFunction, NullaryFunction)} and
     * {@link #computeIfPresent(String, BiFunction, NullaryFunction)}
     * methods.
     *
     * @param key the key of the entry to process
     * @param currentValue the existing value, if any, for {@code key}
     * @param mappingFunction the function that will produce the value. The function will be supplied
     *        with the key and existing value (or null if no entry exists) as parameters. The function should
     *        return the desired new value for the entry or null to remove the entry. If the method throws
     *        an unchecked exception the Store will not be modified (the caller will receive the exception)
     * @param replaceEqual If the existing value in the store is {@link java.lang.Object#equals(Object)} to
     *        the value returned from the mappingFunction this function will be invoked. If this function
     *        returns {@link java.lang.Boolean#FALSE} then the existing entry in the store will not be replaced
     *        with a new entry and the existing entry will have its access time updated
     *
     * @return the new value associated with the key or null if none
     */
    private FakeValueHolder computeInternal(
        final String key,
        final FakeValueHolder currentValue,
        final BiFunction<? super String, ? super String, ? extends String> mappingFunction,
        final NullaryFunction<Boolean> replaceEqual) {

      final String remappedValue = mappingFunction.apply(key, (currentValue == null ? null : currentValue.value()));
      FakeValueHolder newValue = (remappedValue == null ? null : new FakeValueHolder(remappedValue));
      if (newValue == null) {
        /* Remove entry from store */
        this.entries.remove(key);
      } else if (!newValue.equals(currentValue)) {
        /* New, remapped value is different */
        this.entries.put(key, newValue);
      } else {
        /* New, remapped value is the same */
        if (replaceEqual.apply()) {
          /* Replace existing equal value */
          this.entries.put(key, newValue);
        } else {
          /* Update access time of current entry */
          currentValue.lastAccessTime = System.currentTimeMillis();
          newValue = currentValue;
        }
      }
      return newValue;
    }

    @Override
    public ValueHolder<String> compute(
        final String key,
        final BiFunction<? super String, ? super String, ? extends String> mappingFunction,
        final NullaryFunction<Boolean> replaceEqual)
        throws CacheAccessException {
      this.checkFailingKey(key);

      return this.computeInternal(key, this.entries.get(key), mappingFunction, replaceEqual);
    }

    @Override
    public ValueHolder<String> computeIfAbsent(final String key, final Function<? super String, ? extends String> mappingFunction)
        throws CacheAccessException {
      this.checkFailingKey(key);
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
      return this.computeIfPresent(key, remappingFunction, REPLACE_EQUAL_TRUE);
    }

    @Override
    public ValueHolder<String> computeIfPresent(
        final String key,
        final BiFunction<? super String, ? super String, ? extends String> remappingFunction,
        final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
      this.checkFailingKey(key);

      final FakeValueHolder currentValue = this.entries.get(key);
      if (currentValue == null) {
        return null;
      }
      return this.computeInternal(key, this.entries.get(key), remappingFunction, replaceEqual);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This method is implemented as
     * <code>this.{@link #bulkCompute(Set, Function, NullaryFunction)
     *    bulkCompute}(keys, remappingFunction, () -> { returns true; })</code>
     */
    @Override
    public Map<String, ValueHolder<String>> bulkCompute(
        final Set<? extends String> keys,
        final Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> remappingFunction)
        throws CacheAccessException {
      return this.bulkCompute(keys, remappingFunction, REPLACE_EQUAL_TRUE);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation calls {@link #compute(String, BiFunction, NullaryFunction)
     *    compute(key, BiFunction, replaceEqual)} for each key presented in {@code keys}.
     */
    @Override
    public Map<String, org.ehcache.spi.cache.Store.ValueHolder<String>> bulkCompute(
        final Set<? extends String> keys,
        final Function<Iterable<? extends Entry<? extends String, ? extends String>>, Iterable<? extends Entry<? extends String, ? extends String>>> remappingFunction,
        final NullaryFunction<Boolean> replaceEqual)
        throws CacheAccessException {

      final Map<String, ValueHolder<String>> resultMap = new LinkedHashMap<String, ValueHolder<String>>();
      for (final String key : keys) {
        final ValueHolder<String> newValue = this.compute(key,
            new BiFunction<String, String, String>() {
              @Override
              public String apply(final String key, final String oldValue) {
                final Entry<String, String> entry = new AbstractMap.SimpleEntry<String, String>(key, oldValue);
                final Entry<? extends String, ? extends String> remappedEntry =
                    remappingFunction.apply(Collections.singletonList(entry)).iterator().next();
                return remappedEntry.getValue();
              }
            },
            replaceEqual);

        resultMap.put(key, newValue);
      }

      return resultMap;
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

    private void checkFailingKey(final String key) throws CacheAccessException {
      if (this.failingKeys.contains(key)) {
        throw new CacheAccessException(String.format("Accessing failing key: %s", key));
      }
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

      @Override
      public String toString() {
        return "FakeValueHolder{" +
            "value='" + this.value + '\'' +
            ", creationTime=" + this.creationTime +
            ", lastAccessTime=" + this.lastAccessTime +
            '}';
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
   * Provides a basic {@link CacheLoaderWriter} implementation for
   * testing.  The contract implemented by this {@code CacheLoaderWriter} may not be strictly
   * conformant but should be sufficient for {@code Ehcache} implementation testing.
   */
  protected static class FakeCacheLoaderWriter implements CacheLoaderWriter<String, String> {

    private final Map<String, String> entries = new HashMap<String, String>();

    /**
     * Keys for which access results in a thrown {@code Exception}.  This set may be empty.
     */
    private final Set<String> failingKeys;
    
    /**
     * Keys for which success should be reported in {@link BulkCacheLoadingException}
     */
    private Set<String> successKeys;
    
    private boolean isBulkCacheLoadingExceptionEnabled = false;

    /**
     * The entry key causing the {@link #writeAll(Iterable)} and {@link #deleteAll(Iterable)}
     * methods to fail by throwing an exception <i>other</i> than a
     * {@link org.ehcache.exceptions.BulkCacheWritingException BulkCacheWritingException}.
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
          ? Collections.<String>emptySet()
          : Collections.unmodifiableSet(new HashSet<String>(failingKeys)));
    }
    
    public FakeCacheLoaderWriter(final Map<String, String> entries, boolean isBulkCacheLoadingExceptionEnabled) {
      this(entries, Collections.<String>emptySet(), isBulkCacheLoadingExceptionEnabled);
    }
    
    public FakeCacheLoaderWriter(final Map<String, String> entries, final Set<String> successKeys, boolean isBulkCacheLoadingExceptionEnabled) {
      this(entries, Collections.<String>emptySet());
      this.successKeys = successKeys;
      this.isBulkCacheLoadingExceptionEnabled = isBulkCacheLoadingExceptionEnabled;
    }

    Map<String, String> getEntryMap() {
      return Collections.unmodifiableMap(this.entries);
    }

    /**
     * Sets the key causing the {@link #writeAll(Iterable)} and {@link #deleteAll(Iterable)}
     * methods to throw an exception <i>other</i> that a
     * {@link org.ehcache.exceptions.BulkCacheWritingException BulkCacheWritingException}.
     * <p/>
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
     * <p/>
     * If this method throws an exception <i>other</i> than a
     * {@link org.ehcache.exceptions.BulkCacheWritingException BulkCacheWritingException}, the
     * cache image maintained by this {@code CacheLoaderWriter} is in an inconsistent state.
     */
    @Override
    public void writeAll(final Iterable<? extends Map.Entry<? extends String, ? extends String>> entries)
        throws Exception {

      final Set<String> successes = new LinkedHashSet<String>();
      final Map<String, Exception> failures = new LinkedHashMap<String, Exception>();

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
     * <p/>
     * If this method throws an exception <i>other</i> than a
     * {@link org.ehcache.exceptions.BulkCacheWritingException BulkCacheWritingException}, the
     * cache image maintained by this {@code CacheLoaderWriter} is in an inconsistent state.
     */
    @Override
    public void deleteAll(final Iterable<? extends String> keys) throws Exception {
      final Set<String> successes = new LinkedHashSet<String>();
      final Map<String, Exception> failures = new LinkedHashMap<String, Exception>();

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
      if(isBulkCacheLoadingExceptionEnabled) {
        Map<String, Exception> failures = new HashMap<String, Exception>();
        Map<String, String> loadedKeys = new HashMap<String, String>();

        //String loadedVal = "loaded";
        Exception loadingException = new RuntimeException("Exception loading keys");
        
        for (String key : keys) {
          if (!successKeys.contains(key)) {
            failures.put(key, loadingException);
          }else {
            loadedKeys.put(key, null);
          }
          
        }
        
        throw new BulkCacheLoadingException(failures, loadedKeys);
      }
      
      final Map<String, String> resultMap = new HashMap<String, String>();
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