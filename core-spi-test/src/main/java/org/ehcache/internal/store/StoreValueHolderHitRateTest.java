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
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.core.Is.is;

/**
 * Test the {@link Store.ValueHolder#hitRate(long, TimeUnit)} contract of the
 * {@link Store.ValueHolder Store.ValueHolder} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreValueHolderHitRateTest<K, V> extends SPIStoreTester<K, V> {

  public StoreValueHolderHitRateTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void hitRateCanBeReturned()
      throws IllegalAccessException, InstantiationException {
    Store.ValueHolder<V> valueHolder = factory.newValueHolder(factory.createValue(1));

    assertThat(valueHolder.hitRate(TimeUnit.MILLISECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS), anyOf(is(Float.NaN), is(0.0f)));
  }
}
