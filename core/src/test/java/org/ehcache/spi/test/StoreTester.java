package org.ehcache.spi.test;

import org.ehcache.spi.cache.Store;

public class StoreTester<K, V> extends SPITester<K, V> {

  public StoreTester(Store<K, V> store) {
    super(store);
  }
  
  @SPITest
  public void test1() {
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
