package org.ehcache.spi.test;

import org.ehcache.spi.cache.Store;

public class StoreTester<K, V> extends SPITester<Store<K, V>> {

  public StoreTester(final Factory<Store<K, V>> factory) {
    super(factory);
  }

  @SPITest
  public void test1() {
    final Store<K, V> kvStore = factory.newInstance();
    System.out.println("running test1");
  }
  
  @SPITest
  public void test2() {
    System.out.println("running test2");
    throw new RuntimeException("foced failure");
  }
  
  @SPITest
  public void test3() {
    System.out.println("running test3");
  }
}
