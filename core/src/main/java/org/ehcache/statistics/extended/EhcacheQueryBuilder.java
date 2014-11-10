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
package org.ehcache.statistics.extended;

import java.util.HashSet;
import java.util.Set;

import org.ehcache.Ehcache;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.Query;

import static org.terracotta.context.query.QueryBuilder.queryBuilder;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.identifier;
import static org.terracotta.context.query.Matchers.not;
import static org.terracotta.context.query.Matchers.subclassOf;

/**
 * A factory for Ehcache related context query builders.
 * 
 * @author Chris Dennis
 */
class EhcacheQueryBuilder {

  /**
   * Creates a query selecting caches.
   * 
   * @return cache query
   */
  static EhcacheQuery cache() {
    return new EhcacheQuery(queryBuilder().build()).children(Ehcache.class);
  }

  /**
   * Creates a query selecting all children.
   * 
   * @return children query
   */
  static EhcacheQuery children() {
    return new EhcacheQuery(queryBuilder().build()).children();
  }

  /**
   * Creates a query selecting all descendants.
   * 
   * @return descendants query
   */
  static EhcacheQuery descendants() {
    return new EhcacheQuery(queryBuilder().build()).descendants();
  }

  /**
   * Convenience builder for Ehcache related context queries.
   */
  static final class EhcacheQuery implements Query {

    private final Query query;

    private EhcacheQuery(Query query) {
      this.query = query;
    }

    /**
     * Select the children of the current node set
     * 
     * @return children
     */
    EhcacheQuery children() {
      return new EhcacheQuery(queryBuilder().chain(query).children().build());
    }

    /**
     * Select the children of the current node set that are subtypes of the
     * specified class
     * 
     * @param klazz
     *          class to be selected
     * @return children of the specified type
     */
    EhcacheQuery children(Class<?> klazz) {
      return new EhcacheQuery(queryBuilder().chain(query).children()
          .filter(context(identifier(subclassOf(klazz)))).build());
    }

    /**
     * Select the descendants of the current node set
     * 
     * @return descendants
     */
    EhcacheQuery descendants() {
      return new EhcacheQuery(queryBuilder().chain(query).descendants().build());
    }

    /**
     * Add the given nodes to the current node set
     * 
     * @return merged node set
     */
    EhcacheQuery add(final EhcacheQuery chain) {
      return new EhcacheQuery(queryBuilder().chain(query).chain(new Query() {

        @Override
        public Set<TreeNode> execute(Set<TreeNode> input) {
          Set<TreeNode> result = new HashSet<TreeNode>();
          result.addAll(input);
          result.addAll(chain.execute(input));
          return result;
        }
      }).build());
    }

    /**
     * Remove instances of the specified type from the node set
     * 
     * @param klazz
     *          class to be excluded
     * @return current set minus the given type
     */
    EhcacheQuery exclude(Class<?> klazz) {
      return new EhcacheQuery(queryBuilder().chain(query)
          .filter(context(identifier(not(subclassOf(klazz))))).build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<TreeNode> execute(Set<TreeNode> input) {
      return query.execute(input);
    }
  }
}
