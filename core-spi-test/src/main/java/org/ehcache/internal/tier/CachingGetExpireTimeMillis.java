package org.ehcache.internal.tier;

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.function.Function;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.SPITest;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test the {@link CachingTier#getExpireTimeMillis(Store.ValueHolder)} contract of the
 * {@link CachingTier CachingTier} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */
public class CachingGetExpireTimeMillis<K, V> extends CachingTierTester<K, V> {

  private CachingTier tier;

  public CachingGetExpireTimeMillis(final CachingTierFactory<K, V> factory) {
    super(factory);
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
    if (tier != null) {
      tier.close();
      tier = null;
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void getTtiExpirationTime() {
    K key = factory.createKey(1);
    V value= factory.createValue(1);

    final Store.ValueHolder<V> computedValueHolder = mock(Store.ValueHolder.class);
    when(computedValueHolder.value()).thenReturn(value);

    long now = 10L;
    long expirationTimeAmount = 1L;
    TestTimeSource timeSource = new TestTimeSource(now);

    tier = factory.newCachingTier(factory.newConfiguration(factory.getKeyType(), factory.getValueType(),
        2L, null, null, Expirations.timeToIdleExpiration(new Duration(expirationTimeAmount, TimeUnit.MILLISECONDS))), timeSource);

    try {
      Store.ValueHolder<V> valueHolder = tier.getOrComputeIfAbsent(key, new Function<K, Store.ValueHolder<V>>() {
        @Override
        public Store.ValueHolder<V> apply(final K k) {
          return computedValueHolder;
        }
      });

      assertThat(tier.getExpireTimeMillis(valueHolder), is(equalTo(now + expirationTimeAmount)));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  @SuppressWarnings("unchecked")
  public void getTtlExpirationTime() {
    K key = factory.createKey(1);
    V value= factory.createValue(1);

    final Store.ValueHolder<V> computedValueHolder = mock(Store.ValueHolder.class);
    when(computedValueHolder.value()).thenReturn(value);

    long now = 10L;
    long expirationTimeAmount = 1L;
    TestTimeSource timeSource = new TestTimeSource(now);

    tier = factory.newCachingTier(factory.newConfiguration(factory.getKeyType(), factory.getValueType(),
        2L, null, null, Expirations.timeToLiveExpiration(new Duration(expirationTimeAmount, TimeUnit.MILLISECONDS))), timeSource);

    try {
      Store.ValueHolder<V> valueHolder = tier.getOrComputeIfAbsent(key, new Function<K, Store.ValueHolder<V>>() {
        @Override
        public Store.ValueHolder<V> apply(final K k) {
          return computedValueHolder;
        }
      });

      assertThat(tier.getExpireTimeMillis(valueHolder), is(equalTo(now + expirationTimeAmount)));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }


  @SPITest
  @SuppressWarnings("unchecked")
  public void exceptionWhenValueHolderIsNotAnInstanceFromTheCachingTier() {
    Store.ValueHolder<V> valueHolder = mock(Store.ValueHolder.class);

    tier = factory.newCachingTier(factory.newConfiguration(factory.getKeyType(), factory.getValueType(),
        1L, null, null, Expirations.noExpiration()));

    try {
      tier.getExpireTimeMillis(valueHolder);
      throw new AssertionError();
    } catch (ClassCastException e) {
      // expected
    }
  }
}
