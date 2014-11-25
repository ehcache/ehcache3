package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.loader.CacheLoaderFactory;
import org.ehcache.spi.service.CacheWriterConfiguration;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class LoaderEhcacheITest {

  private CacheManager cacheManager;
  private CacheLoader<? super Number, ? super CharSequence> cacheLoader;

  @Before
  public void setUp() throws Exception {
    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheLoaderFactory cacheLoaderFactory = mock(CacheLoaderFactory.class);
    cacheLoader = mock(CacheLoader.class);
    when(cacheLoaderFactory.createCacheLoader(anyString(), (CacheConfiguration<Number, CharSequence>) anyObject())).thenReturn((CacheLoader) cacheLoader);
    builder.using(cacheLoaderFactory);
    cacheManager = builder.build();
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testSimpleGetWithLoader() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().addServiceConfig(new CacheWriterConfiguration()).buildConfig(Number.class, CharSequence.class));

    when(cacheLoader.load(eq(1))).thenReturn("lala");

    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("lala"));
    assertThat(testCache.get(2), is(nullValue()));

    verify(cacheLoader, times(1)).load(eq(1));
    verify(cacheLoader, times(1)).load(eq(2));
  }

  @Test
  public void testSimpleGetAllWithLoader() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().addServiceConfig(new CacheWriterConfiguration()).buildConfig(Number.class, CharSequence.class));

    when(cacheLoader.loadAll((Iterable)any())).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Iterable<Integer> iterable = (Iterable) invocation.getArguments()[0];
        for (Integer i : iterable) {

          switch (i) {
            case 1:
              return Collections.singletonMap(1, "one");
            case 2:
              return Collections.singletonMap(2, "two");
            default:
              return Collections.singletonMap(3, null);
          }
        }

        throw new AssertionError("iterable must have a least 1 element");
      }
    });

    Map<Number, CharSequence> all = testCache.getAll(Arrays.asList(1, 2, 3));
    assertThat(all.size(), is(3));
    assertThat(all.get(1), Matchers.<CharSequence>equalTo("val#1"));
    assertThat(all.get(2), Matchers.<CharSequence>equalTo("val#2"));
    assertThat(all.get(3), is(Matchers.nullValue()));
  }

}
