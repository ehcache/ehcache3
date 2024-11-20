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

import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.test.SPITest;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

/**
 * Test the {@link Store.ValueHolder#creationTime(java.util.concurrent.TimeUnit)} contract of the
 * {@link Store.ValueHolder Store.ValueHolder} interface.
 *
 * @author Aurelien Broszniowski
 */

public class StoreValueHolderCreationTimeTest<K, V> extends SPIStoreTester<K, V> {

  public StoreValueHolderCreationTimeTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void creationTimeCanBeReturned()
      throws IllegalAccessException, InstantiationException {
    Store.ValueHolder<V> valueHolder = factory.newValueHolder(factory.createValue(1));

    assertThat(valueHolder.creationTime(), is(notNullValue()));
  }

}
