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

import org.ehcache.Cache;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Expirations;
import org.ehcache.function.Function;
import org.ehcache.function.Predicate;
import org.ehcache.function.Predicates;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.Ignore;
import org.ehcache.spi.test.SPITest;
import org.hamcrest.Matchers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.lang.Long;
import java.util.Set;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Test the {@link org.ehcache.spi.cache.Store#bulkCompute(Iterable, org.ehcache.function.Function)} contract of the
 * {@link org.ehcache.spi.cache.Store Store} interface.
 * <p/>
 *
 * @author Gaurav Mangalick
 */

public class StoreBulkComputeIfAbsentTest<K, V> extends SPIStoreTester<K, V> {

  public StoreBulkComputeIfAbsentTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void remappingFunctionReturnsIterableOfEntriesForEachInputEntry() throws Exception {
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void missingIterableEntriesAreIgnoredByTheStore() throws Exception {
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testWrongKeyType() throws Exception {
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void remappingFunctionGetsIterableWithMappedStoreEntryValueOrNull() throws Exception {
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void computeValuesForEveryKeyUsingARemappingFunction() throws Exception {
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testRemappingFunctionProducesWrongKeyType() throws Exception {
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testRemappingFunctionProducesWrongValueType() throws Exception {
  }
}
