package org.ehcache;

import org.junit.Test;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class EhCachingProviderTest {
  
  @Test
  public void testLoadsAsCachingProvider() {
    final CachingProvider provider = Caching.getCachingProvider();
    assertThat(provider, is(instanceOf(EhCachingProvider.class)));
  }
}
