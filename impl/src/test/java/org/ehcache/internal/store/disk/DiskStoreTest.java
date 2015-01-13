package org.ehcache.internal.store.disk;

import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.spi.cache.Store;
import org.junit.Test;

/**
 * Created by lorban on 12/01/15.
 */
public class DiskStoreTest {

  @Test
  public void testMisc() throws Exception {
    StoreConfigurationImpl<Integer, String> configuration = new StoreConfigurationImpl<Integer, String>(Integer.class, String.class, null, null, null, getClass().getClassLoader(), null, null);
    DiskStore<Integer, String> diskStore = new DiskStore<Integer, String>(configuration, "diskStore", SystemTimeSource.INSTANCE);
    diskStore.init();

    System.out.println("value: " + getValue(diskStore, 1));

    diskStore.put(1, "one");

    System.out.println("value: " + getValue(diskStore, 1));

    diskStore.close();
  }

  private String getValue(DiskStore<Integer, String> diskStore, Integer key) throws org.ehcache.exceptions.CacheAccessException {
    Store.ValueHolder<String> stringValueHolder = diskStore.get(key);
    if (stringValueHolder == null) return null;
    return stringValueHolder.value();
  }
}
