package org.ehcache;

import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Ludovic Orban
 */
public class EhcacheTest {

  @Test
  public void testPutAll() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(store);

    ehcache.putAll(new HashMap<Number, CharSequence>() {{
      put(1, "one");
      put(2, "two");
      put(3, "three");
    }}.entrySet());

    verify(store).compute(eq(1), any(BiFunction.class));
    verify(store).compute(eq(2), any(BiFunction.class));
    verify(store).compute(eq(3), any(BiFunction.class));
  }

  @Test
  public void testGetAll() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(store);

    ehcache.getAll(Arrays.asList(1, 2, 3));

    verify(store).computeIfAbsent(eq(1), any(Function.class));
    verify(store).computeIfAbsent(eq(2), any(Function.class));
    verify(store).computeIfAbsent(eq(3), any(Function.class));
  }

  @Test
  public void testRemoveAll() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(store);

    ehcache.removeAll(Arrays.asList(1, 2, 3));

    verify(store).remove(eq(1));
    verify(store).remove(eq(2));
    verify(store).remove(eq(3));
  }

}
