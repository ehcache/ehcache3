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

package org.ehcache.core;

import org.ehcache.Cache;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.exceptions.StorePassThroughException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.core.util.TestCacheConfig;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.StoreAccessException;
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

import java.util.AbstractMap;
import java.util.ArrayList;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;

/**
 * Provides testing of basic CRUD operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public abstract class EhcacheBasicCrudBase {

  protected static final CacheConfiguration<String, String> CACHE_CONFIGURATION = new TestCacheConfig<>(String.class, String.class);

  @Mock
  protected Store<String, String> store;
  @Mock
  protected CacheEventDispatcher<String, String> cacheEventDispatcher;
  @Mock
  protected ResilienceStrategy<String, String> resilienceStrategy;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }

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
   * Provides a basic {@link Store} implementation for testing.
   * The contract implemented by this {@code Store} is not strictly conformant but
   * should be sufficient for {@code Ehcache} implementation testing.
   */
  protected static class FakeStore implements Store<String, String> {

    private final CacheConfigurationChangeListener cacheConfigurationChangeListener = event -> {
      // noop
    };

    private static final Supplier<Boolean> REPLACE_EQUAL_TRUE = () -> true;
    private static final Supplier<Boolean> INVOKE_WRITER_FALSE = () -> false;

    /**
     * The key:value pairs served by this {@code Store}.  This map may be empty.
     */
    private final Map<String, FakeValueHolder> entries;

    /**
     * Keys for which access results in a thrown {@code Exception}.  This set may be empty.
     */
    private final Set<String> failingKeys;

    public FakeStore(final Map<String, String> entries) {
      this(entries, Collections.emptySet());
    }

    public FakeStore(final Map<String, String> entries, final Set<String> failingKeys) {
      assert failingKeys != null;

      // Use of ConcurrentHashMap is required to avoid ConcurrentModificationExceptions using Iterator.remove
      this.entries = new ConcurrentHashMap<>();
      if (entries != null) {
        for (final Map.Entry<String, String> entry : entries.entrySet()) {
          this.entries.put(entry.getKey(), new FakeValueHolder(entry.getValue()));
        }
      }
      this.failingKeys = Collections.unmodifiableSet(new HashSet<>(failingKeys));
    }

    /**
     * Gets a mapping of the entries in this {@code Store}.
     *
     * @return a new, unmodifiable map of the entries in this {@code Store}.
     */
    protected Map<String, String> getEntryMap() {
      Map<String, String> result = new HashMap<>(entries.size());
      entries.forEach((k, v) -> result.put(k, v.get()));
      return Collections.unmodifiableMap(result);
    }

    @Override
    public ValueHolder<String> get(final String key) throws StoreAccessException {
      this.checkFailingKey(key);
      final FakeValueHolder valueHolder = this.entries.get(key);
      if (valueHolder != null) {
        valueHolder.lastAccessTime = System.currentTimeMillis();
      }
      return valueHolder;
    }

    @Override
    public boolean containsKey(final String key) throws StoreAccessException {
      this.checkFailingKey(key);
      return this.entries.containsKey(key);
    }

    @Override
    public PutStatus put(final String key, final String value) throws StoreAccessException {
      this.checkFailingKey(key);
      FakeValueHolder toPut = new FakeValueHolder(value);
      this.entries.put(key, toPut);
      return PutStatus.PUT;
    }

    @Override
    public ValueHolder<String> putIfAbsent(final String key, final String value, Consumer<Boolean> put) throws StoreAccessException {
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
    public boolean remove(final String key) throws StoreAccessException {
      this.checkFailingKey(key);
      if (this.entries.remove(key) == null) {
        return false;
      }
      return true;
    }

    @Override
    public RemoveStatus remove(final String key, final String value) throws StoreAccessException {
      this.checkFailingKey(key);
      final ValueHolder<String> currentValue = this.entries.get(key);
      if (currentValue == null) {
        return RemoveStatus.KEY_MISSING;
      } else if (!currentValue.get().equals(value)) {
        return RemoveStatus.KEY_PRESENT;
      }
      this.entries.remove(key);
      return RemoveStatus.REMOVED;
    }

    @Override
    public ValueHolder<String> replace(final String key, final String value) throws StoreAccessException {
      this.checkFailingKey(key);
      final ValueHolder<String> currentValue = this.entries.get(key);
      if (currentValue != null) {
        this.entries.put(key, new FakeValueHolder(value));
      }
      return currentValue;
    }

    @Override
    public ReplaceStatus replace(final String key, final String oldValue, final String newValue) throws StoreAccessException {
      this.checkFailingKey(key);
      final ValueHolder<String> currentValue = this.entries.get(key);
      if (currentValue == null) {
        return ReplaceStatus.MISS_NOT_PRESENT;
      }
      if (!currentValue.get().equals(oldValue)) {
        return ReplaceStatus.MISS_PRESENT;
      }
      this.entries.put(key, new FakeValueHolder(newValue));
      return ReplaceStatus.HIT;
    }

    @Override
    public void clear() throws StoreAccessException {
      this.entries.clear();
    }

    @Override
    public StoreEventSource<String, String> getStoreEventSource() {
      throw new UnsupportedOperationException("TODO Implement me!");
    }

    /**
     * {@inheritDoc}
     * <p>
     * The {@code Iterator} returned by this method <b>does not</b> have a {@code remove}
     * method.  The {@code Iterator} returned by {@code FakeStore.this.entries.entrySet().iterator()}
     * must not throw {@link java.util.ConcurrentModificationException ConcurrentModification}.
     */
    @Override
    public Iterator<Cache.Entry<String, ValueHolder<String>>> iterator() {

      return new Iterator<Cache.Entry<String, ValueHolder<String>>>() {

        final java.util.Iterator<Map.Entry<String, FakeValueHolder>> iterator =
            FakeStore.this.entries.entrySet().iterator();

        @Override
        public boolean hasNext() {
          return this.iterator.hasNext();
        }

        @Override
        public Cache.Entry<String, ValueHolder<String>> next() throws StoreAccessException {

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

          };
        }
      };
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method is implemented as
     * <code>this.{@link Store#getAndCompute(Object, BiFunction)} (keys, mappingFunction, () -> { returns true; })</code>
     */
    @Override
    public ValueHolder<String> getAndCompute(final String key, final BiFunction<? super String, ? super String, ? extends String> mappingFunction)
        throws StoreAccessException {
      return this.computeAndGet(key, mappingFunction, REPLACE_EQUAL_TRUE, INVOKE_WRITER_FALSE);
    }

    /**
     * Common core for the {@link Store#computeAndGet(Object, BiFunction, Supplier, Supplier)} method.
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
        final Supplier<Boolean> replaceEqual) throws StoreAccessException {

      String remappedValue;
      try {
        remappedValue = mappingFunction.apply(key, (currentValue == null ? null : currentValue.get()));
      } catch (StorePassThroughException cpte) {
        Throwable cause = cpte.getCause();
        if(cause instanceof RuntimeException) {
          throw   (RuntimeException) cause;
        } else if(cause instanceof StoreAccessException){
          throw   (StoreAccessException) cause;
        } else {
          throw new StoreAccessException(cause);
        }
      }
      FakeValueHolder newValue = (remappedValue == null ? null : new FakeValueHolder(remappedValue));
      if (newValue == null) {
        /* Remove entry from store */
        this.entries.remove(key);
      } else if (!newValue.equals(currentValue)) {
        /* New, remapped value is different */
        this.entries.put(key, newValue);
      } else {
        /* New, remapped value is the same */
        if (replaceEqual.get()) {
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
    public ValueHolder<String> computeAndGet(
            final String key,
            final BiFunction<? super String, ? super String, ? extends String> mappingFunction,
            final Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter)
        throws StoreAccessException {
      this.checkFailingKey(key);

      return this.computeInternal(key, this.entries.get(key), mappingFunction, replaceEqual);
    }

    @Override
    public ValueHolder<String> computeIfAbsent(final String key, final Function<? super String, ? extends String> mappingFunction)
        throws StoreAccessException {
      this.checkFailingKey(key);
      FakeValueHolder currentValue = this.entries.get(key);
      if (currentValue == null) {
        String newValue;
        try {
          newValue = mappingFunction.apply(key);
        } catch (StorePassThroughException cpte) {
          Throwable cause = cpte.getCause();
          if(cause instanceof RuntimeException) {
            throw   (RuntimeException) cause;
          } else if(cause instanceof StoreAccessException){
            throw   (StoreAccessException) cause;
          } else {
            throw new StoreAccessException(cause);
          }
        }
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

    /**
     * {@inheritDoc}
     * <p>
     * This method is implemented as
     * <code>this.{@link #bulkCompute(Set, Function, Supplier)
     *    bulkCompute}(keys, remappingFunction, () -> { returns true; })</code>
     */
    @Override
    public Map<String, ValueHolder<String>> bulkCompute(
        final Set<? extends String> keys,
        final Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> remappingFunction)
        throws StoreAccessException {
      return this.bulkCompute(keys, remappingFunction, REPLACE_EQUAL_TRUE);
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation calls {@link Store#computeAndGet(Object, BiFunction, Supplier, Supplier)
     *    } for each key presented in {@code keys}.
     */
    @Override
    public Map<String, Store.ValueHolder<String>> bulkCompute(
        final Set<? extends String> keys,
        final Function<Iterable<? extends Entry<? extends String, ? extends String>>, Iterable<? extends Entry<? extends String, ? extends String>>> remappingFunction,
        final Supplier<Boolean> replaceEqual)
        throws StoreAccessException {

      final Map<String, ValueHolder<String>> resultMap = new LinkedHashMap<>();
      for (final String key : keys) {
        final ValueHolder<String> newValue = this.computeAndGet(key,
          (key1, oldValue) -> {
            final Entry<String, String> entry = new AbstractMap.SimpleEntry<>(key1, oldValue);
            final Entry<? extends String, ? extends String> remappedEntry =
                remappingFunction.apply(Collections.singletonList(entry)).iterator().next();
            return remappedEntry.getValue();
          },
            replaceEqual, INVOKE_WRITER_FALSE);

        resultMap.put(key, newValue);
      }

      return resultMap;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation is based, in part, on the implementation found in
     * {@code org.ehcache.internal.store.OnHeapStore}.  This implementation calls
     * {@code mappingFunction} for each key through an internal function supplied
     * to {@link #computeIfAbsent(String, Function) computeIfAbsent}.
     */
    @Override
    public Map<String, ValueHolder<String>> bulkComputeIfAbsent(final Set<? extends String> keys, final Function<Iterable<? extends String>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> mappingFunction)
        throws StoreAccessException {
      final Map<String, ValueHolder<String>> resultMap = new LinkedHashMap<>();
      for (final String key : keys) {
        final ValueHolder<String> newValue = this.computeIfAbsent(key, key1 -> {
          final Entry<? extends String, ? extends String> entry =
              mappingFunction.apply(Collections.singleton(key1)).iterator().next();
          return entry.getValue();
        });
        resultMap.put(key, newValue);
      }
      return resultMap;
    }

    @Override
    public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
      List<CacheConfigurationChangeListener> configurationChangeListenerList
          = new ArrayList<>();
      configurationChangeListenerList.add(this.cacheConfigurationChangeListener);
      return configurationChangeListenerList;
    }

    private void checkFailingKey(final String key) throws StoreAccessException {
      if (this.failingKeys.contains(key)) {
        throw new StoreAccessException(String.format("Accessing failing key: %s", key));
      }
    }

    /**
     * A {@link Store.ValueHolder} implementation for use within
     * {@link org.ehcache.core.EhcacheBasicCrudBase.FakeStore}.
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
      public String get() {
        return this.value;
      }

      @Override
      public long creationTime() {
        return creationTime;
      }

      @Override
      public long expirationTime() {
        return 0;
      }

      @Override
      public boolean isExpired(long expirationTime) {
        return false;
      }

      @Override
      public long lastAccessTime() {
        return lastAccessTime;
      }

      @Override
      public long getId() {
        throw new UnsupportedOperationException("Implement me!");
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
