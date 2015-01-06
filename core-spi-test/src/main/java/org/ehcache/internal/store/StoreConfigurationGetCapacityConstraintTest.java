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
import static org.hamcrest.Matchers.is;

/**
 * Test the {@link org.ehcache.spi.cache.Store.Configuration#getCapacityConstraint()} contract of the
 * {@link org.ehcache.spi.cache.Store.Configuration Store.Configuration} interface.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */

public class StoreConfigurationGetCapacityConstraintTest<K, V> extends SPIStoreTester<K, V> {

  public StoreConfigurationGetCapacityConstraintTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void returnsCorrectCapacityConstraint() throws IllegalAccessException, InstantiationException {
    Long capacityConstraint = 100L;
    Store.Configuration<K, V> kvConfiguration = factory.newConfiguration(factory.getKeyType(), factory.getValueType(),
        capacityConstraint, null, null);

    assertThat(capacityConstraint, is(equalTo((kvConfiguration.getCapacityConstraint()))));
  }
}
