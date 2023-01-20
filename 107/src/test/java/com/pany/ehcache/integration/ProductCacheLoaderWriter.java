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

package com.pany.ehcache.integration;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

import com.pany.domain.Product;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Alex Snaps
 */
public class ProductCacheLoaderWriter implements CacheLoaderWriter<Long, Product> {

  public static final ConcurrentMap<Long, List<Product>> written = new ConcurrentHashMap<Long, List<Product>>();
  public static final Set<Long> seen = new HashSet<Long>();

  @Override
  public Product load(final Long key) throws Exception {
    seen.add(key);
    return new Product(key);
  }

  @Override
  public Map<Long, Product> loadAll(final Iterable<? extends Long> keys) throws Exception {
    for (Long key : keys) {
      seen.add(key);
    }
    return Collections.emptyMap();
  }

  @Override
  public void write(final Long key, final Product value) throws Exception {
    List<Product> products = written.get(key);
    if(products == null) {
      products = new ArrayList<Product>();
      final List<Product> previous = written.putIfAbsent(key, products);
      if(previous != null) {
        products = previous;
      }
    }
    products.add(value);
  }

  @Override
  public void writeAll(final Iterable<? extends Map.Entry<? extends Long, ? extends Product>> entries) throws Exception {
    // no-op
  }

  @Override
  public void delete(final Long key) throws Exception {
    // no-op
  }

  @Override
  public void deleteAll(final Iterable<? extends Long> keys) throws Exception {
    // no-op
  }
}
