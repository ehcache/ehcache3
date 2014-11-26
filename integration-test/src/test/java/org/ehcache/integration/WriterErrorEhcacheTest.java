package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.exceptions.BulkCacheWriterException;
import org.ehcache.exceptions.CacheWriterException;
import org.ehcache.spi.writer.CacheWriter;
import org.ehcache.spi.writer.CacheWriterFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.fail;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class WriterErrorEhcacheTest {

  private CacheManager cacheManager;
  private Cache<Number, CharSequence> testCache;
  private CacheWriter<? super Number, ? super CharSequence> cacheWriter;

  @Before
  public void setUp() throws Exception {
    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheWriterFactory cacheWriterFactory = mock(CacheWriterFactory.class);
    cacheWriter = mock(CacheWriter.class);
    when(cacheWriterFactory.createCacheWriter(anyString(), (CacheConfiguration<Number, CharSequence>) anyObject())).thenReturn((CacheWriter) cacheWriter);
    builder.using(cacheWriterFactory);
    cacheManager = builder.build();
    testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testPutWithWriterException() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheWriter).write(eq(1), eq("one"));

    try {
      testCache.put(1, "one");
      fail("expected CacheWriterException");
    } catch (CacheWriterException ex) {
      // expected
    }
    testCache.put(2, "two");

    verify(cacheWriter, times(1)).write(eq(2), eq("two"));
  }

  @Test
  public void testRemoveWithWriterException() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheWriter).delete(eq(1));

    try {
      testCache.remove(1);
      fail("expected CacheWriterException");
    } catch (CacheWriterException ex) {
      // expected
    }
  }

  @Test
  public void testRemoveAllWithWriterException() throws Exception {
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Iterable<Integer> iterable = (Iterable) invocation.getArguments()[0];
        Set<Integer> result = new HashSet<Integer>();

        for (Integer i : iterable) {
          switch (i) {
            case 2:
              throw new Exception("Mock Exception: cannot write 2");
            default:
              result.add(i);
              break;
          }
        }

        return result;
      }
    }).when(cacheWriter).deleteAll((Iterable) Matchers.any());

    try {
      testCache.removeAll(Arrays.asList(1, 2, 3, 4));
      fail("expected CacheWriterException");
    } catch (BulkCacheWriterException ex) {
      assertThat(ex.getFailures().size(), is(1));
      assertThat(ex.getSuccesses().size(), is(3));
    }
  }

  @Test
  public void testRemove2ArgsWithWriterException() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheWriter).delete(eq(1), eq("one"));

    //TODO: shouldn't remove "gate" (in Chris D. parlance) on the cache?
    try {
      testCache.remove(1, "one");
      fail("expected CacheWriterException");
    } catch (CacheWriterException ex) {
      // expected
    }
  }

  @Test
  public void testReplace2ArgsWithWriterException() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheWriter).write(eq(1), eq("one#2"));

    testCache.put(1, "one");
    try {
      testCache.replace(1, "one#2");
      fail("expected CacheWriterException");
    } catch (CacheWriterException ex) {
      // expected
    }
  }

  @Test
  public void testReplace3ArgsWithWriterException() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheWriter).write(eq(1), eq("one"), eq("one#2"));

    //TODO: shouldn't replace "gate" (in Chris D. parlance) on the cache?
    try {
      testCache.replace(1, "one", "one#2");
      fail("expected CacheWriterException");
    } catch (CacheWriterException ex) {
      // expected
    }
  }

  @Test
  public void testPutIfAbsentWithWriterException() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheWriter).write(eq(1), Matchers.<CharSequence>eq(null), eq("one"));

    try {
      testCache.putIfAbsent(1, "one");
      fail("expected CacheWriterException");
    } catch (CacheWriterException ex) {
      // expected
    }

    testCache.put(2, "two");
    testCache.putIfAbsent(2, "two#2");
  }

  @Test
  public void testPutAllWithWriterException() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheWriter).writeAll(Matchers.<Iterable>any());

    Map<Integer, String> values = new HashMap<Integer, String>();
    values.put(1, "one");
    values.put(2, "two");

    try {
      testCache.putAll(values.entrySet());
      fail("expected CacheWriterException");
    } catch (CacheWriterException ex) {
      // expected
    }
  }

}
