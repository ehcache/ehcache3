package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.exceptions.BulkCacheLoaderException;
import org.ehcache.exceptions.CacheLoaderException;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.loader.CacheLoaderFactory;
import org.ehcache.spi.service.CacheWriterConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;
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
public class LoaderErrorEhcacheTest {

  private CacheManager cacheManager;
  private Cache<Number, CharSequence> testCache;
  private CacheLoader<? super Number, ? super CharSequence> cacheLoader;

  @Before
  public void setUp() throws Exception {
    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheLoaderFactory cacheLoaderFactory = mock(CacheLoaderFactory.class);
    cacheLoader = mock(CacheLoader.class);
    when(cacheLoaderFactory.createCacheLoader(anyString(), (CacheConfiguration<Number, CharSequence>) anyObject())).thenReturn((CacheLoader) cacheLoader);
    builder.using(cacheLoaderFactory);
    cacheManager = builder.build();
    testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().addServiceConfig(new CacheWriterConfiguration()).buildConfig(Number.class, CharSequence.class));
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testGetWithLoaderException() throws Exception {
    when(cacheLoader.load(eq(1))).thenThrow(new Exception("TestException: cannot load data"));

    try {
      testCache.get(1);
      fail("expected CacheLoaderException");
    } catch (CacheLoaderException ex) {
      // expected
    }

    verify(cacheLoader, times(1)).load(eq(1));
  }

  @Test
  public void testGetAllWithLoaderException() throws Exception {
    when(cacheLoader.loadAll((Iterable)any())).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Iterable<Integer> iterable = (Iterable) invocation.getArguments()[0];

        Map<Number, CharSequence> result = new HashMap<Number, CharSequence>();

        for (Integer i : iterable) {
          switch (i) {
            case 1:
              result.put(1, "one");
              break;
            case 2:
              throw new Exception("Mock Exception: cannot load 2");
            case 3:
              result.put(3, "three");
              break;
            default:
              result.put(i, null);
              break;
          }
        }

        return result;
      }
    });

    try {
      testCache.getAll(Arrays.asList(1, 2, 3, 4));
      fail("expected BulkCacheLoaderException");
    } catch (BulkCacheLoaderException ex) {
      assertThat(ex.getFailures().size(), is(1));
      assertThat(ex.getSuccesses().size(), is(1)); //TODO: does that make sense? 1 was a failure, 2 were loadable, a 3rd one was not found without error
    }
  }

}
