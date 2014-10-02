package org.ehcache.internal.store;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import org.ehcache.spi.cache.Store.ValueHolder;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author vfunshteyn
 *
 */
public class OnHeapStoreValueHolderTest {

  @Test
  public void testValue() {
    Object o = "foo";
    ValueHolder<?> vh1 = new OnHeapStoreValueHolder<Object>(o);
    ValueHolder<?> vh2 = new OnHeapStoreValueHolder<Object>(o);
    assertSame(vh1.value(), vh2.value());
  }

  @Test
  public void testLastAccessTime() throws InterruptedException {
    ValueHolder<Integer> vh = new OnHeapStoreValueHolder<Integer>(10);
    long start = vh.lastAccessTime(TimeUnit.MILLISECONDS);
    Thread.sleep(10); // I don't like it but think it's appropriate here
    vh.value();
    assertTrue(vh.lastAccessTime(TimeUnit.MILLISECONDS) > start);
  }

  @Test
  public void testEquals() {
    ValueHolder<Integer> vh = new OnHeapStoreValueHolder<Integer>(10);
    assertThat(new OnHeapStoreValueHolder<Integer>(10), is(vh));
  }
  
  @Test
  public void testNotEquals() {
    ValueHolder<Integer> vh = new OnHeapStoreValueHolder<Integer>(10);
    assertThat(new OnHeapStoreValueHolder<Integer>(101), not(vh));
  }

  @Test(expected=NullPointerException.class)
  public void testNullValue() {
    new OnHeapStoreValueHolder<Object>(null);
  }
}
