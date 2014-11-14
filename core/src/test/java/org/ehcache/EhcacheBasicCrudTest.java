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
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.CacheLoaderException;
import org.ehcache.exceptions.ExceptionFactory;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.statistics.CacheOperationOutcomes;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.ValueStatistic;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Provides testing of basic CRUD operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheBasicCrudTest {

  private static final CacheConfiguration<String, String> CACHE_CONFIGURATION =
      CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(String.class, String.class);

  @Mock
  private Store<String, String> store;

  @Mock
  private CacheLoader<String, String> cacheLoader;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }


  @Test
  public void testPut() {

  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#get(Object)} for a missing key.
   */
  @Test
  public void testGetNoStoreEntryNoCacheLoader() throws Exception {
    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.get("key"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), any(Function.class));
    verify(this.store, never()).remove("key");
    this.validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS_NOT_FOUND));
  }

  @Test
  public void testGetNoStoreEntryNoCacheLoaderEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    assertThat(ehcache.get("key"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), any(Function.class));
    verify(this.store, never()).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    this.validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.MISS_NOT_FOUND));
  }

  @Test
  public void testGetNoStoreEntryHasCacheLoaderEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    when(this.cacheLoader.load("key")).thenReturn("value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    assertThat(ehcache.get("key"), is("value"));
    verify(this.store).computeIfAbsent(eq("key"), any(Function.class));
    verify(this.store, never()).remove("key");
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    this.validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT));
  }

  @Test
  public void testGetNoStoreEntryCacheLoaderException() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);

    when(this.cacheLoader.load("key")).thenThrow(ExceptionFactory.newCacheLoaderException(new Exception()));
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    try {
      ehcache.get("key");
      fail();
    } catch (CacheLoaderException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), any(Function.class));
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    this.validateStats(ehcache, EnumSet.noneOf(CacheOperationOutcomes.GetOutcome.class));
  }

  @Test
  public void testGetNoStoreEntryCacheAccessExceptionNoCacheLoader() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), any(Function.class));

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.get("key"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), any(Function.class));
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    this.validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  @Test
  public void testGetNoStoreEntryCacheAccessExceptionNoCacheLoaderEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), any(Function.class));

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    assertThat(ehcache.get("key"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), any(Function.class));
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    this.validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  @Test
  public void testGetNoStoreEntryCacheAccessExceptionHasCacheLoaderEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), any(Function.class));

    when(this.cacheLoader.load("key")).thenReturn("value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    assertThat(ehcache.get("key"), is("value"));
    verify(this.store).computeIfAbsent(eq("key"), any(Function.class));
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    this.validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  @Test
  public void testGetNoStoreEntryCacheAccessExceptionCacheLoaderException() throws Exception {
    final MockStore realStore = new MockStore(Collections.<String, String>emptyMap());
    this.store = spy(realStore);
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), any(Function.class));

    when(this.cacheLoader.load("key")).thenThrow(ExceptionFactory.newCacheLoaderException(new Exception()));
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    try {
      ehcache.get("key");
      fail();
    } catch (CacheLoaderException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), any(Function.class));
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    this.validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  @Test
  public void testGetHasStoreEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);
    assertThat(realStore.getMap().get("key"), equalTo("value"));

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), any(Function.class));
    verify(this.store, never()).remove("key");
    this.validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT));
  }

  @Test
  public void testGetHasStoreEntryCacheAccessExceptionNoCacheLoader() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), any(Function.class));

    final Ehcache<String, String> ehcache = this.getEhcache(null);

    assertThat(ehcache.get("key"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), any(Function.class));
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    this.validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  @Test
  public void testGetHasStoreEntryCacheAccessExceptionNoCacheLoaderEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), any(Function.class));

    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    assertThat(ehcache.get("key"), is(nullValue()));
    verify(this.store).computeIfAbsent(eq("key"), any(Function.class));
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    this.validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  @Test
  public void testGetHasStoreEntryCacheAccessExceptionHasCacheLoaderEntry() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), any(Function.class));

    when(this.cacheLoader.load("key")).thenReturn("value");
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    assertThat(ehcache.get("key"), equalTo("value"));
    verify(this.store).computeIfAbsent(eq("key"), any(Function.class));
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    this.validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  @Test
  public void testGetHasStoreEntryCacheAccessExceptionCacheLoaderException() throws Exception {
    final MockStore realStore = new MockStore(Collections.singletonMap("key", "value"));
    this.store = spy(realStore);
    assertThat(realStore.getMap().get("key"), equalTo("value"));
    doThrow(new CacheAccessException("")).when(this.store).computeIfAbsent(eq("key"), any(Function.class));

    when(this.cacheLoader.load("key")).thenThrow(ExceptionFactory.newCacheLoaderException(new Exception()));
    final Ehcache<String, String> ehcache = this.getEhcache(this.cacheLoader);

    try {
      ehcache.get("key");
      fail();
    } catch (CacheLoaderException e) {
      // Expected
    }
    verify(this.store).computeIfAbsent(eq("key"), any(Function.class));
    verify(this.store, times(1)).remove("key");
    assertThat(realStore.getMap().containsKey("key"), is(false));
    this.validateStats(ehcache, EnumSet.of(CacheOperationOutcomes.GetOutcome.FAILURE));
  }

  /**
   * Validates expected {@link org.terracotta.statistics.OperationStatistic} updates for the
   * indicated {@code Ehcache} instance.  The statistics identified in {@code changed} are
   * checked for a value of {@code 1}; all other statistics in the same enumeration class are
   * checked for a value of {@code 0}.
   *
   * @param ehcache the {@code Ehcache} instance to check
   * @param changed the
   * @param <E>
   */
  private <E extends Enum<E>> void validateStats(final Ehcache<?, ?> ehcache, final EnumSet<E> changed) {
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

    final OperationStatistic<E> operationStatistic = this.getOperationStatistic(ehcache, statsClass);
    for (final E statId : changed) {
      assertThat(this.getStatistic(operationStatistic, statId), StatisticMatcher.equalTo(1L));
    }
    for (final E statId : unchanged) {
      assertThat(this.getStatistic(operationStatistic, statId), StatisticMatcher.equalTo(0L));
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
  private <E extends Enum<E>> OperationStatistic<E> getOperationStatistic(final Ehcache<?, ?> ehcache, final Class<E> statsClass) {
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
  private <E extends Enum<E>> Number getStatistic(final OperationStatistic<E> operationStatistic, final E statId) {
    if (operationStatistic != null) {
      final ValueStatistic<Long> valueStatistic = operationStatistic.statistic(statId);
      return (valueStatistic == null ? null : valueStatistic.value());
    }
    return null;
  }

  /**
   * Gets an initialized {@link org.ehcache.Ehcache Ehcache} instance using the
   * {@link org.ehcache.spi.loader.CacheLoader CacheLoader} provided.
   *
   * @param cacheLoader
   *    the {@code CacheLoader} to use; may be {@code null}
   *
   * @return a new {@code Ehcache} instance
   *
   */
  private Ehcache<String, String> getEhcache(final CacheLoader<String, String> cacheLoader) {
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(CACHE_CONFIGURATION, this.store, cacheLoader);
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));
    return ehcache;
  }

  /**
   * Provides a basic {@link org.ehcache.spi.cache.Store} implementation for testing.
   * The contract implemented by this {@code Store} is not strictly conformant but
   * should be sufficient for {@code Ehcache} implementation testing.
   */
  // TODO: Use a validated Store implementation.
  private static class MockStore implements Store<String, String> {

    private final Map<String, ValueHolder<String>> entries;

    public MockStore(final Map<String, String> entries) {
      this.entries = new HashMap<String, ValueHolder<String>>();
      if (entries != null) {
        for (final Map.Entry<String, String> entry : entries.entrySet()) {
          this.entries.put(entry.getKey(), new MockValueHolder(entry.getValue()));
        }
      }
    }

    /**
     * Gets a mapping of the entries in this {@code Store}.
     *
     * @return a new, unmodifiable map of the entries in this {@code Store}.
     */
    private Map<String, String> getMap() {
      final Map<String, String> result = new HashMap<String, String>();
      for (final Map.Entry<String, ValueHolder<String>> entry : this.entries.entrySet()) {
        result.put(entry.getKey(), entry.getValue().value());
      }
      return Collections.unmodifiableMap(result);
    }

    @Override
    public ValueHolder<String> get(final String key) throws CacheAccessException {
      return this.entries.get(key);
    }

    @Override
    public boolean containsKey(final String key) throws CacheAccessException {
      return this.entries.containsKey(key);
    }

    @Override
    public void put(final String key, final String value) throws CacheAccessException {
      this.entries.put(key, new MockValueHolder(value));
    }

    @Override
    public ValueHolder<String> putIfAbsent(final String key, final String value) throws CacheAccessException {
      final ValueHolder<String> currentValue = this.entries.get(key);
      if (currentValue == null) {
        this.entries.put(key, new MockValueHolder(value));
        return null;
      }
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
        this.entries.put(key, new MockValueHolder(value));
      }
      return currentValue;
    }

    @Override
    public boolean replace(final String key, final String oldValue, final String newValue) throws CacheAccessException {
      final ValueHolder<String> currentValue = this.entries.get(key);
      if (currentValue != null && currentValue.value().equals(oldValue)) {
        this.entries.put(key, new MockValueHolder(newValue));
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
    public Iterator<Cache.Entry<String, ValueHolder<String>>> iterator() throws CacheAccessException {
      throw new UnsupportedOperationException();
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
      final MockValueHolder newValueHolder = new MockValueHolder(newValue);
      this.entries.put(key, newValueHolder);
      return newValueHolder;
    }

    @Override
    public ValueHolder<String> computeIfAbsent(final String key, final Function<? super String, ? extends String> mappingFunction)
        throws CacheAccessException {
      ValueHolder<String> currentValue = this.entries.get(key);
      if (currentValue == null) {
        final String newValue = mappingFunction.apply(key);
        if (newValue != null) {
          final MockValueHolder newValueHolder = new MockValueHolder(newValue);
          this.entries.put(key, newValueHolder);
          currentValue = newValueHolder;
        }
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
          final MockValueHolder newValueHolder = new MockValueHolder(newValue);
          this.entries.put(key, newValueHolder);
          return newValueHolder;
        }
      }
      return null;
    }

    @Override
    public Map<String, ValueHolder<String>> bulkCompute(final Iterable<? extends String> keys, final Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> remappingFunction)
        throws CacheAccessException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, ValueHolder<String>> bulkComputeIfAbsent(final Iterable<? extends String> keys, final Function<Iterable<? extends String>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> mappingFunction)
        throws CacheAccessException {
      throw new UnsupportedOperationException();
    }

    private static class MockValueHolder implements ValueHolder<String>  {

      private final String value;
      private final long creationTime;
      private long lastAccessTime;

      public MockValueHolder(final String value) {
        this.value = value;
        this.creationTime = System.currentTimeMillis();
      }

      @Override
      public String value() {
        this.lastAccessTime = System.currentTimeMillis();
        return this.value;
      }

      @Override
      public long creationTime(final TimeUnit unit) {
        return this.creationTime;
      }

      @Override
      public long lastAccessTime(final TimeUnit unit) {
        return this.lastAccessTime;
      }

      @Override
      public float hitRate(final TimeUnit unit) {
        return 0;
      }
    }
  }

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