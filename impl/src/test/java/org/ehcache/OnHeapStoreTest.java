package org.ehcache;

import org.ehcache.internal.store.OnHeapStore;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.Factory;
import org.ehcache.spi.test.StoreTester;
import org.junit.Test;

public class OnHeapStoreTest {

  @Test
  public void test() throws Exception {
    StoreTester<String, String> tester = new StoreTester<String, String>(new Factory<Store<String, String>>() {
      @Override
      public Store<String, String> newInstance() {
        return new OnHeapStore<String, String>();
      }
    });
    tester.runTestSuite().reportAndThrow();
  }

}
