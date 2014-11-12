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
package org.ehcache;

import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Provides testing of basic CRUD operations on an {@code Ehcache}.
 *
 * @author Clifford W. Johnson
 */
public class EhcacheBasicCrudTest {

  @Mock
  private Store<String, String> store;

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }


  @Test
  public void testPut() {

  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#get(Object)} for a missing key.
   */
  @Test
  public void testGetKeyMissing() {
    final Ehcache<String, String> ehcache = new Ehcache<String, String>(
        CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(String.class, String.class), this.store, null);
    ehcache.init();
    assertThat("cache not initialized", ehcache.getStatus(), is(Status.AVAILABLE));

    assertThat(ehcache.get("key"), is(nullValue()));
    // TODO: Add verification of GetOutcome
  }

  /**
   * Tests the effect of a {@link org.ehcache.Ehcache#get(Object)} call for a key found
   * in the {@link org.ehcache.spi.cache.Store}.
   */
  @Test
  public void testGetKeyInStore() {

  }

  @Test
  public void testGetKeyInCacheLoader() {

  }

  @Test
  public void testGetKeyInCache() {

  }

}