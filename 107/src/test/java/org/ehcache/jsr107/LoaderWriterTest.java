package org.ehcache.jsr107;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.spi.CachingProvider;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * @author Ludovic Orban
 */
public class LoaderWriterTest {

  private CacheLoader<Number, CharSequence> cacheLoader;
  private CacheWriter<Number, CharSequence> cacheWriter;
  private Cache<Number, CharSequence> testCache;
  private CacheManager cacheManager;

  @Before
  public void setUp() throws Exception {
    cacheLoader = mock(CacheLoader.class);
    cacheWriter = mock(CacheWriter.class);

    CachingProvider provider = Caching.getCachingProvider();
    cacheManager = provider.getCacheManager(this.getClass().getResource("/ehcache-loader-writer-107.xml").toURI(), getClass().getClassLoader());

    testCache = cacheManager.createCache("testCache", new MutableConfiguration<Number, CharSequence>()
        .setReadThrough(true)
        .setWriteThrough(true)
        .setCacheLoaderFactory(new Factory<CacheLoader<Number, CharSequence>>() {
          @Override
          public CacheLoader<Number, CharSequence> create() {
            return cacheLoader;
          }
        })
        .setCacheWriterFactory(new Factory<CacheWriter<? super Number, ? super CharSequence>>() {
          @Override
          public CacheWriter<? super Number, ? super CharSequence> create() {
            return cacheWriter;
          }
        })
        .setTypes(Number.class, CharSequence.class));
  }

  @After
  public void tearDown() throws Exception {
    cacheManager.close();
  }

  @Test
  public void testSimplePutIfAbsentWithLoaderAndWriter_absent() throws Exception {
    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.putIfAbsent(1, "one"), is(true));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));

    verifyZeroInteractions(cacheLoader);
    verify(cacheWriter, times(1)).write(eq(new Eh107CacheWriter.Entry<Number, CharSequence>(1, "one")));
  }

  @Test
  public void testSimplePutIfAbsentWithLoaderAndWriter_existsInSor() throws Exception {
    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.putIfAbsent(1, "one"), is(true));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));

    verifyZeroInteractions(cacheLoader);
    verify(cacheWriter, times(1)).write(eq(new Eh107CacheWriter.Entry<Number, CharSequence>(1, "one")));
  }

  @Test
  public void testSimplePutIfAbsentWithLoaderAndWriter_existsInStore() throws Exception {
    testCache.put(1, "un");
    reset(cacheWriter);

    assertThat(testCache.putIfAbsent(1, "one"), is(false));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("un"));

    verifyZeroInteractions(cacheLoader);
    verifyZeroInteractions(cacheWriter);
  }

}
