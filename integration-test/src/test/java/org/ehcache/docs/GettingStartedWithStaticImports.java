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

package org.ehcache.docs;

import org.ehcache.CacheManager;
import org.junit.Test;

// CSOFF: AvoidStaticImport
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
// CSON: AvoidStaticImport

/**
 * Samples to get started with Ehcache 3. Java 7 syntax
 */
@SuppressWarnings("unused")
public class GettingStartedWithStaticImports {

  @Test
  public void cachemanagerExample() {
    // tag::java7Example[]
    try(CacheManager cacheManager = newCacheManagerBuilder() // <1>
      .withCache("preConfigured", newCacheConfigurationBuilder(Long.class, String.class, heap(10))) // <2>
      .build(true)) { // <3>

      // Same code as before [...]
    }
    // end::java7Example[]
  }
}
