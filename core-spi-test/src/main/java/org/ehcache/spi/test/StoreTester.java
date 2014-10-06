/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.spi.test;

import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.spi.cache.Store;

public class StoreTester extends SPITester {

  final StoreFactory storeFactory;

  public StoreTester(final StoreFactory factory) {
    this.storeFactory = factory;
  }

  @SPITest
  public void test1() {
    System.out.println("running test1");
    final Store<String, String> store = storeFactory.newStore(new StoreConfigurationImpl<String, String>(String.class, String.class));
  }
  
  @SPITest
  public void test2() {
    System.out.println("running test2");
    final Store<Integer, String> store = storeFactory.newStore(new StoreConfigurationImpl<Integer, String>(Integer.class, String.class));
  }
  
  @SPITest
  public void test3() {
    System.out.println("running test3");
  }
}
