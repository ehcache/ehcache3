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

import org.ehcache.Ehcache;
import org.ehcache.statistics.CacheOperationOutcomes;
import org.terracotta.context.query.Query;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.ehcache.statistics.extended.EhcacheQueryBuilder.cache;
import static org.ehcache.statistics.extended.EhcacheQueryBuilder.children;
import static org.ehcache.statistics.extended.EhcacheQueryBuilder.descendants;


/**
 * The Enum OperationType.
 *
 * @author cdennis
 */
enum StandardOperationStatistic {
    CACHE_LOADER(false, cache(), CacheOperationOutcomes.CacheLoaderOutcome.class, "cacheLoader", "cache"),
  
    /** The cache get. */
    CACHE_GET(true, cache(), CacheOperationOutcomes.GetOutcome.class, "get", "cache"),

    /** The cache put. */
    CACHE_PUT(true, cache(), CacheOperationOutcomes.PutOutcome.class, "put", "cache"),

    /** The cache remove. */
    CACHE_REMOVE(true, cache(), CacheOperationOutcomes.RemoveOutcome.class, "remove", "cache"),
    
    /** The cache remove(K, V) */
    CACHE_CONDITIONAL_REMOVE(true, cache(), CacheOperationOutcomes.ConditionalRemoveOutcome.class, "conditionalRemove", "cache"),
    
    /** The cache putIfAbsent. */
    CACHE_PUT_IF_ABSENT(true, cache(), CacheOperationOutcomes.PutIfAbsentOutcome.class, "putIfAbsent", "cache"),
    
    /** The cache replace. */
    CACHE_REPLACE(true, cache(), CacheOperationOutcomes.ReplaceOutcome.class, "replace", "cache"),      

    /** The evicted. */
    EVICTION(false, cache().add(children().exclude(Ehcache.class).add(descendants())), CacheOperationOutcomes.EvictionOutcome.class, "eviction");
    
    private final boolean required;
    private final Query context;
    private final Class<? extends Enum<?>> type;
    private final String name;
    private final Set<String> tags;

    private StandardOperationStatistic(Class<? extends Enum<?>> type, String name, String ... tags) {
        this(false, type, name, tags);
    }

    private StandardOperationStatistic(boolean required, Class<? extends Enum<?>> type, String name, String ... tags) {
        this(required, descendants(), type, name, tags);
    }

    private StandardOperationStatistic(boolean required, Query context, Class<? extends Enum<?>> type, String name, String ... tags) {
        this.required = required;
        this.context = context;
        this.type = type;
        this.name = name;
        this.tags = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(tags)));
    }
    
    /**
     * If this statistic is required.
     * <p>
     * If required and this statistic is not present an exception will be thrown.
     *
     * @return
     */
    final boolean required() {
        return required;
    }

    /**
     * Query that select context nodes for this statistic.
     * 
     * @return context query
     */
    final Query context() {
        return context;
    }
    
    /**
     * Operation result type.
     *
     * @return operation result type
     */
    @SuppressWarnings("rawtypes")
    final Class<? extends Enum> type() {
        return type;
    }

    /**
     * The name of the statistic as found in the statistics context tree.
     *
     * @return the statistic name
     */
    final String operationName() {
        return name;
    }

    /**
     * A set of tags that will be on the statistic found in the statistics context tree.
     *
     * @return the statistic tags
     */
    final Set<String> tags() {
        return tags;
    }
    
    /**
     * Is this stat search related or not?
     * @return
     */
    boolean isSearch() {
        return false;
    }
}
