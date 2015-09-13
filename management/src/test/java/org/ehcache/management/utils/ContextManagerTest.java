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
package org.ehcache.management.utils;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.Ehcache;
import org.ehcache.EhcacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.management.registry.DefaultManagementRegistry;
import org.junit.Test;
import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.Matcher;
import org.terracotta.context.query.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.terracotta.context.query.Matchers.attributes;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.hasAttribute;
import static org.terracotta.context.query.QueryBuilder.queryBuilder;

/**
 * @author Ludovic Orban
 */
public class ContextManagerTest {

  @Test
  public void testFindCacheNames() throws Exception {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache1", cacheConfiguration)
        .withCache("cache2", cacheConfiguration)
        .using(new DefaultManagementRegistry())
        .build(true);

    try {
      cacheManager.createCache("cache3", cacheConfiguration);

      Collection<String> cacheNames = findCacheNames((EhcacheManager) cacheManager);
      assertThat(cacheNames.size(), is(3));
      assertThat(cacheNames, containsInAnyOrder("cache1", "cache2", "cache3"));
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testFindCacheName() throws Exception {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache1", cacheConfiguration)
        .withCache("cache2", cacheConfiguration)
        .using(new DefaultManagementRegistry())
        .build(true);

    try {
      Cache<Long, String> cache3 = cacheManager.createCache("cache3", cacheConfiguration);

      String cacheName = findCacheName((Ehcache<?, ?>) cache3);
      assertThat(cacheName, equalTo("cache3"));
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testFindCacheManagerNameByCacheManager() throws Exception {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache1", cacheConfiguration)
        .withCache("cache2", cacheConfiguration)
        .using(new DefaultManagementRegistry())
        .build(true);

    try {
      cacheManager.createCache("cache3", cacheConfiguration);

      String cacheManagerName = findCacheManagerName((EhcacheManager) cacheManager);
      assertThat(cacheManagerName, startsWith("cache-manager-"));
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testFindCacheManagerNameByCache() throws Exception {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("cache1", cacheConfiguration)
        .withCache("cache2", cacheConfiguration)
        .using(new DefaultManagementRegistry())
        .build(true);

    try {
      cacheManager.createCache("cache3", cacheConfiguration);

      String cacheManagerName = findCacheManagerName((Ehcache<?, ?>) cacheManager.getCache("cache1", Long.class, String.class));
      assertThat(cacheManagerName, startsWith("cache-manager-"));
    } finally {
      cacheManager.close();
    }
  }

  public static Collection<String> findCacheNames(EhcacheManager ehcacheManager) {
    Query q = queryBuilder().descendants()
        .filter(context(attributes(hasAttribute("CacheName", new Matcher<String>() {
          @Override
          protected boolean matchesSafely(String object) {
            return true;
          }
        }))))
        .filter(context(attributes(hasAttribute("tags", new Matcher<Set<String>>() {
          @Override
          protected boolean matchesSafely(Set<String> object) {
            return object.containsAll(Arrays.asList("cache", "exposed"));
          }
        }))))
        .build();

    Set<TreeNode> queryResult = ehcacheManager.getStatisticsManager().query(q);

    Collection<String> result = new ArrayList<String>();
    for (TreeNode treeNode : queryResult) {
      String cacheName = (String) treeNode.getContext().attributes().get("CacheName");
      result.add(cacheName);
    }
    return result;
  }

  public static String findCacheName(Ehcache<?, ?> ehcache) {
    Query query = queryBuilder().children().filter(context(attributes(hasAttribute("tags", new Matcher<Set<String>>() {
      @Override
      protected boolean matchesSafely(Set<String> object) {
        return object.containsAll(Arrays.asList("cache", "exposed"));
      }
    })))).build();

    Set<TreeNode> queryResult = query.execute(Collections.singleton(ContextManager.nodeFor(ehcache)));
    if (queryResult.size() != 1) {
      throw new RuntimeException("Cache without name setting : " + ehcache);
    }
    TreeNode treeNode = queryResult.iterator().next();
    return (String) treeNode.getContext().attributes().get("CacheName");
  }

  public static String findCacheManagerName(EhcacheManager ehcacheManager) {
    Query q = queryBuilder().descendants()
        .filter(context(attributes(hasAttribute("CacheManagerName", new Matcher<String>() {
          @Override
          protected boolean matchesSafely(String object) {
            return true;
          }
        }))))
        .filter(context(attributes(hasAttribute("tags", new Matcher<Set<String>>() {
          @Override
          protected boolean matchesSafely(Set<String> object) {
            return object.containsAll(Arrays.asList("cacheManager", "exposed"));
          }
        }))))
        .build();

    Set<TreeNode> queryResult = ehcacheManager.getStatisticsManager().query(q);
    if (queryResult.size() != 1) {
      throw new RuntimeException("Cache manager without name setting : " + ehcacheManager);
    }
    TreeNode treeNode = queryResult.iterator().next();
    return (String) treeNode.getContext().attributes().get("CacheManagerName");
  }

  public static String findCacheManagerName(Ehcache<?, ?> ehcache) {
    Query query = queryBuilder().parent().descendants()
        .filter(context(attributes(hasAttribute("tags", new Matcher<Set<String>>() {
          @Override
          protected boolean matchesSafely(Set<String> object) {
            return object.containsAll(Arrays.asList("cacheManager", "exposed"));
          }
        })))).build();

    Set<TreeNode> queryResult = query.execute(Collections.singleton(ContextManager.nodeFor(ehcache)));
    if (queryResult.size() != 1) {
      throw new RuntimeException("Cache manager without name setting from cache : " + ehcache);
    }
    TreeNode treeNode = queryResult.iterator().next();
    return (String) treeNode.getContext().attributes().get("CacheManagerName");
  }

}
