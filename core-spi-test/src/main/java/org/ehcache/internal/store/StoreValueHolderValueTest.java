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

package org.ehcache.internal.store;

import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.SPITest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

/**
 * Test the {@link org.ehcache.spi.cache.Store.ValueHolder#value()} contract of the
 * {@link org.ehcache.spi.cache.Store Store.ValueHolder} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreValueHolderValueTest<K, V> extends SPIStoreTester<K, V> {

  public StoreValueHolderValueTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void valueIsHeldByValueHolder()
      throws IllegalAccessException, InstantiationException {
    V value = factory.getValueType().newInstance();
    Store.ValueHolder<V> valueHolder = factory.newValueHolder(value);

    assertThat(valueHolder.value(), is(equalTo(value)));
  }

  @SPITest
  public void valueHolderCanThrowException()
      throws IllegalAccessException, InstantiationException {
    V value = factory.getValueType().newInstance();
    try {
      Store.ValueHolder<V> valueHolder = factory.newValueHolder(value);
    } catch (Exception e) {
      // This will not compile if the Exception is not thrown
      // TODO to update with relevant exception when javadoc is defined
    }
  }
}
