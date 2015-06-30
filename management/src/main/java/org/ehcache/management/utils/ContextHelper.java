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

import org.ehcache.Ehcache;
import org.ehcache.EhcacheManager;
import org.terracotta.context.ContextManager;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.Matcher;
import org.terracotta.context.query.Query;
import org.terracotta.context.query.QueryBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.terracotta.context.query.Matchers.attributes;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.hasAttribute;
import static org.terracotta.context.query.QueryBuilder.queryBuilder;

/**
 * @author Ludovic Orban
 */
public abstract class ContextHelper {

  private ContextHelper() {
  }

  public static Collection<String> findCacheNames(EhcacheManager ehcacheManager) {
    Query q = QueryBuilder.queryBuilder().descendants()
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
    Query q = QueryBuilder.queryBuilder().descendants()
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
