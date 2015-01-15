package org.ehcache.internal.store.disk;

import org.ehcache.Cache;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.expiry.Expirations;
import org.ehcache.function.BiFunction;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.spi.cache.Store;
import org.junit.Test;

/**
 * Created by lorban on 12/01/15.
 */
public class DiskStoreTest {

  @Test
  public void testMisc() throws Exception {

    EvictionVeto<Integer, String> evictionVeto = new EvictionVeto<Integer, String>() {
      @Override
      public boolean test(Cache.Entry<Integer, String> argument) {
        return argument.getKey() < 3;
      }
    };
    StoreConfigurationImpl<Integer, String> configuration = new StoreConfigurationImpl<Integer, String>(Integer.class, String.class, 2L, evictionVeto, null, getClass().getClassLoader(), Expirations.noExpiration(), new JavaSerializationProvider());
    DiskStore<Integer, String> diskStore = new DiskStore<Integer, String>(configuration, "diskStore", SystemTimeSource.INSTANCE);
    diskStore.init();

    System.out.println("value: " + getValue(diskStore, 1));

    diskStore.put(1, "one");

    System.out.println("value: " + getValue(diskStore, 1));

    Store.ValueHolder<String> computed = diskStore.compute(2, new BiFunction<Integer, String, String>() {
      @Override
      public String apply(Integer k, String v) {
        String result;
        if (v == null || !v.equals("two")) {
          result = "two";
        } else {
          result = "deux";
        }
        System.out.println("compute : " + k + "/" + v + " -> " + result);
        return result;
      }
    });
    System.out.println("computed : " + computed);

    for (int i=0;i<10;i++) {
      diskStore.put(i, "val#" + i);
    }

    Thread.sleep(100);

    System.out.println("\nContents");
    Store.Iterator<Cache.Entry<Integer, Store.ValueHolder<String>>> it = diskStore.iterator();
    while (it.hasNext()) {
      Cache.Entry<Integer, Store.ValueHolder<String>> next = it.next();
      System.out.println(next.getKey() + "/" + next.getValue());
    }

    diskStore.close();
  }

  private String getValue(DiskStore<Integer, String> diskStore, Integer key) throws org.ehcache.exceptions.CacheAccessException {
    Store.ValueHolder<String> stringValueHolder = diskStore.get(key);
    if (stringValueHolder == null) return null;
    return stringValueHolder.value();
  }
}
