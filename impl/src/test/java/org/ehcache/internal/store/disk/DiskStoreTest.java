package org.ehcache.internal.store.disk;

import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.expiry.Expirations;
import org.ehcache.function.BiFunction;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.spi.cache.Store;
import org.junit.Test;

/**
 * Created by lorban on 12/01/15.
 */
public class DiskStoreTest {

  @Test
  public void testMisc() throws Exception {
    StoreConfigurationImpl<Integer, String> configuration = new StoreConfigurationImpl<Integer, String>(Integer.class, String.class, null, null, null, getClass().getClassLoader(), Expirations.noExpiration(), null);
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

    diskStore.close();
  }

  private String getValue(DiskStore<Integer, String> diskStore, Integer key) throws org.ehcache.exceptions.CacheAccessException {
    Store.ValueHolder<String> stringValueHolder = diskStore.get(key);
    if (stringValueHolder == null) return null;
    return stringValueHolder.value();
  }
}
