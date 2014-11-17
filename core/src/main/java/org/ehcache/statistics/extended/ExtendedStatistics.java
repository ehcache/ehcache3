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

import org.ehcache.statistics.CacheOperationOutcomes;
import org.ehcache.statistics.StoreOperationOutcomes;
import org.ehcache.statistics.extended.ExtendedStatistics.Operation;
import org.terracotta.statistics.archive.Timestamped;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The ExtendedStatistics interface.
 *
 * @author cschanck
 */
public interface ExtendedStatistics {

    /** The Constant ALL_CACHE_PUT_OUTCOMES. */
    static final Set<CacheOperationOutcomes.PutOutcome> ALL_CACHE_PUT_OUTCOMES = EnumSet.allOf(CacheOperationOutcomes.PutOutcome.class);

    /** The Constant ALL_CACHE_GET_OUTCOMES. */
    static final Set<CacheOperationOutcomes.GetOutcome> ALL_CACHE_GET_OUTCOMES = EnumSet.allOf(CacheOperationOutcomes.GetOutcome.class);

    /** The Constant ALL_CACHE_MISS_OUTCOMES. */
    static final Set<CacheOperationOutcomes.GetOutcome> ALL_CACHE_MISS_OUTCOMES = EnumSet.of(
            CacheOperationOutcomes.GetOutcome.FAILURE, CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER, CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER);

    /** The Constant ALL_STORE_PUT_OUTCOMES. */
    static final Set<StoreOperationOutcomes.PutOutcome> ALL_STORE_PUT_OUTCOMES = EnumSet.allOf(StoreOperationOutcomes.PutOutcome.class);

    /** The Constant ALL_CACHE_REMOVE_OUTCOMES. */
    static final Set<CacheOperationOutcomes.RemoveOutcome> ALL_CACHE_REMOVE_OUTCOMES = EnumSet.allOf(CacheOperationOutcomes.RemoveOutcome.class);
    
    /** The Constant GET_WITH_LOADER_OUTCOMES. */
    static final Set<CacheOperationOutcomes.GetOutcome> GET_WITH_LOADER_OUTCOMES = EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER, CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER);

    /** The Constant GET_NO_LOADER_OUTCOMES. */
    static final Set<CacheOperationOutcomes.GetOutcome> GET_NO_LOADER_OUTCOMES = EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER, CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER);
    
    /** The Constant CACHE_LOADER_OUTCOMES. */
    static final Set<CacheOperationOutcomes.CacheLoaderOutcome> ALL_CACHE_LOADER_OUTCOMES = EnumSet.allOf(CacheOperationOutcomes.CacheLoaderOutcome.class);
    
    /**
     * Sets the time to disable.
     *
     * @param time the time
     * @param unit the unit
     */
    void setTimeToDisable(long time, TimeUnit unit);

    /**
     * Sets the always on.
     *
     * @param alwaysOn the new always on
     */
    void setAlwaysOn(boolean alwaysOn);

    /**
     * Gets the.
     *
     * @return the operation
     */
    Operation<CacheOperationOutcomes.GetOutcome> get();

    /**
     * Put.
     *
     * @return the operation
     */
    Operation<CacheOperationOutcomes.PutOutcome> put();

    /**
     * Removes the.
     *
     * @return the operation
     */
    Operation<CacheOperationOutcomes.RemoveOutcome> remove();
    

    /**
     * remove(K, V)
     *
     * @return the operation
     */
    Operation<CacheOperationOutcomes.ConditionalRemoveOutcome> conditionalRemove();

    /**
     * Eviction.
     *
     * @return the operation
     */
    Operation<CacheOperationOutcomes.EvictionOutcome> eviction();
    
    /**
     * PutIfAbsent
     *
     * @return the operation
     */
    Operation<CacheOperationOutcomes.PutIfAbsentOutcome> putIfAbsent();
    
    /**
     * Replace
     *
     * @return the operation
     */
    Operation<CacheOperationOutcomes.ReplaceOutcome> replace();    

    /**
     * All get.
     *
     * @return the result
     */
    Result allGet();

    /**
     * All miss.
     *
     * @return the result
     */
    Result allMiss();

    /**
     * All put.
     *
     * @return the result
     */
    Result allPut();

    /**
     * All remove.
     *
     * @return the result
     */
    Result allRemove();
    
    /**
     * Get with cacheLoader
     * @return result
     */
    Result getWithLoader();
    
    /**
     * Get without cacheLoader
     * @return result
     */
    Result getNoLoader();
    
    /**
     * cache loader dedicated stats
     * @return result
     */
    Result cacheLoader();
    
    /**
     * Cache hit ratio.
     * @return the statistic
     */
    Statistic<Double> cacheHitRatio();

    /**
     * Operations.
     *
     * @param <T> the generic type
     * @param outcome the outcome
     * @param name the name
     * @param tags the tags
     * @return the sets the
     */
    <T extends Enum<T>> Set<Operation<T>> operations(Class<T> outcome, String name, String... tags);

    /**
     * The Interface Operation.
     *
     * @param <T> the generic type
     */
    public interface Operation<T extends Enum<T>> {

        /**
         * Type.
         *
         * @return the class
         */
        Class<T> type();

        /**
         * Component.
         *
         * @param result the result
         * @return the result
         */
        Result component(T result);

        /**
         * Compound.
         *
         * @param results the results
         * @return the result
         */
        Result compound(Set<T> results);

        /**
         * Ratio of.
         *
         * @param numerator the numerator
         * @param denomiator the denomiator
         * @return the statistic
         */
        Statistic<Double> ratioOf(Set<T> numerator, Set<T> denomiator);

        /**
         * Sets the always on.
         *
         * @param enable the new always on
         */
        void setAlwaysOn(boolean enable);

        /**
         * Checks if is always on.
         *
         * @return true, if is always on
         */
        boolean isAlwaysOn();

        /**
         * Sets the window.
         *
         * @param time the time
         * @param unit the unit
         */
        void setWindow(long time, TimeUnit unit);

        /**
         * Sets the history.
         *
         * @param samples the samples
         * @param time the time
         * @param unit the unit
         */
        void setHistory(int samples, long time, TimeUnit unit);

        /**
         * Gets the window size.
         *
         * @param unit the unit
         * @return the window size
         */
        long getWindowSize(TimeUnit unit);

        /**
         * Gets the history sample size.
         *
         * @return the history sample size
         */
        int getHistorySampleSize();

        /**
         * Gets the history sample time.
         *
         * @param unit the unit
         * @return the history sample time
         */
        long getHistorySampleTime(TimeUnit unit);

    }

    /**
     * The Interface Result.
     */
    public interface Result {

        /**
         * Count.
         *
         * @return the statistic
         */
        Statistic<Long> count();

        /**
         * Rate.
         *
         * @return the statistic
         */
        Statistic<Double> rate();

        /**
         * Latency.
         *
         * @return the latency
         */
        Latency latency();
    }

    /**
     * The Latency interface. Provides min/max/average.
     */
    public interface Latency {

        /**
         * Minimum latency observed.
         *
         * @return Minimum observed latency. NULL if no operation was observed.
         */
        Statistic<Long> minimum();

        /**
         * Maximum latency observed.
         *
         * @return Maximum observed latency. NULL if no operation was observed.
         */
        Statistic<Long> maximum();

        /**
         * Average observed latency.
         *
         * @return Average observed latency. NULL if no operation was observed.
         */
        Statistic<Double> average();
    }

    /**
     * The Interface Statistic.
     *
     * @param <T> the generic type
     */
    public interface Statistic<T extends Number> {

        /**
         * Active.
         *
         * @return true, if successful
         */
        boolean active();

        /**
         * Value.
         *
         * @return the t
         */
        T value();

        /**
         * History.
         *
         * @return the list
         */
        List<Timestamped<T>> history();
    }

}