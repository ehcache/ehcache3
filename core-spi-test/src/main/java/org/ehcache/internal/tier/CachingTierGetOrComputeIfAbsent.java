package org.ehcache.internal.tier;

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.function.Function;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.Ignore;
import org.ehcache.spi.test.SPITest;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Test the {@link org.ehcache.spi.cache.tiering.CachingTier#getOrComputeIfAbsent(Object, Function)} contract of the
 * {@link org.ehcache.spi.cache.tiering.CachingTier CachingTier} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class CachingTierGetOrComputeIfAbsent<K, V> extends SPICachingTierTester<K, V> {

  protected CachingTier<K, V> tier;

  public CachingTierGetOrComputeIfAbsent(final CachingTierFactory<K, V> factory) {
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
  public void nonMarkedMappingIsEvictable() {
  }
}
