package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.spi.service.CacheWriterConfiguration;
import org.ehcache.spi.writer.CacheWriter;
import org.ehcache.spi.writer.CacheWriterFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class WriterEhcacheITest {

  private CacheManager cacheManager;
  private CacheWriter<? super Number, ? super CharSequence> cacheWriter;

  @Before
  public void setUp() throws Exception {
    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheWriterFactory cacheWriterFactory = mock(CacheWriterFactory.class);
    cacheWriter = mock(CacheWriter.class);
    when(cacheWriterFactory.createCacheWriter(anyString(), (CacheConfiguration<Number, CharSequence>) anyObject())).thenReturn((CacheWriter) cacheWriter);
    builder.using(cacheWriterFactory);
    cacheManager = builder.build();
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testSimplePutWithWriter() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().addServiceConfig(new CacheWriterConfiguration()).buildConfig(Number.class, CharSequence.class));

    testCache.put(1, "one");
    testCache.put(2, "two");

    verify(cacheWriter, times(1)).write(eq(1), eq("one"));
    verify(cacheWriter, times(1)).write(eq(2), eq("two"));
  }

  @Test
  public void testSimplePutIfAbsentWithWriter() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().addServiceConfig(new CacheWriterConfiguration()).buildConfig(Number.class, CharSequence.class));

    testCache.putIfAbsent(1, "one");
    testCache.putIfAbsent(2, "two");
    testCache.putIfAbsent(2, "two#2");

    verify(cacheWriter, times(1)).write(eq(1), Matchers.<String>eq(null), eq("one"));
    verify(cacheWriter, times(1)).write(eq(2), Matchers.<String>eq(null), eq("two"));
  }

  @Test
  public void testSimplePutAllWithWriter() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));

    Map<Integer, String> values = new HashMap<Integer, String>();
    values.put(1, "one");
    values.put(2, "two");

    testCache.putAll(values.entrySet());

    verify(cacheWriter, times(1)).writeAll(argThat(contains(entry(1, "one"))));
    verify(cacheWriter, times(1)).writeAll(argThat(contains(entry(2, "two"))));
  }

  private static Map.Entry<Number, CharSequence> entry(Number number, CharSequence charSequence) {
    return new AbstractMap.SimpleEntry<Number, CharSequence>(number, charSequence);
  }

}
