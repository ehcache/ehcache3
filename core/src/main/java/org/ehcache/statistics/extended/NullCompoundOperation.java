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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.ehcache.statistics.extended.ExtendedStatistics.Latency;
import org.ehcache.statistics.extended.ExtendedStatistics.Operation;
import org.ehcache.statistics.extended.ExtendedStatistics.Result;
import org.ehcache.statistics.extended.ExtendedStatistics.Statistic;

import org.terracotta.statistics.archive.Timestamped;


/**
 * The Class NullCompoundOperation.
 *
 * @param <T> the generic type
 * @author cdennis
 */
final class NullCompoundOperation<T extends Enum<T>> implements Operation<T> {

    private static final Operation INSTANCE = new NullCompoundOperation();

    private NullCompoundOperation() {
        //singleton
    }

    /**
     * Instance.
     *
     * @param <T> the generic type
     * @return the operation
     */
    static <T extends Enum<T>> Operation<T> instance(Class<T> klazz) {
        return INSTANCE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<T> type() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result component(T result) {
        return NullOperation.instance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result compound(Set<T> results) {
        return NullOperation.instance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statistic<Double> ratioOf(Set<T> numerator, Set<T> denomiator) {
        return NullStatistic.instance(Double.NaN);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAlwaysOn(boolean enable) {
        //no-op
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setWindow(long time, TimeUnit unit) {
        //no-op
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setHistory(int samples, long time, TimeUnit unit) {
        //no-op
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAlwaysOn() {
        // no-op
        return false;
    }

    /*
     * {@inheritDoc}
     */
    /* (non-Javadoc)
     * @see net.sf.ehcache.statistics.extended.ExtendedStatistics.Operation#getWindowSize(java.util.concurrent.TimeUnit)
     */
    @Override
    public long getWindowSize(TimeUnit unit) {
        // no-op
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getHistorySampleSize() {
        // no-op
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getHistorySampleTime(TimeUnit unit) {
        // no-op
        return 0;
    }
}

/**
 * Null result object
 *
 * @author cdennis
 *
 */
final class NullOperation implements Result {

    private static final Result INSTANCE = new NullOperation();

    /**
     * Instantiates a new null operation.
     */
    private NullOperation() {
        //singleton
    }

    /**
     * Instance method
     * @return
     */
    static final Result instance() {
        return INSTANCE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statistic<Long> count() {
        return NullStatistic.instance(0L);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statistic<Double> rate() {
        return NullStatistic.instance(Double.NaN);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Latency latency() throws UnsupportedOperationException {
        return NullLatency.instance();
    }
}

/**
 * Noop latency class
 *
 * @author cdennis
 *
 */
final class NullLatency implements Latency {

    private static final Latency INSTANCE = new NullLatency();

    /**
     * Private constructor
     */
    private NullLatency() {
    }

    /**
     * Instance accessor
     * @return
     */
    static Latency instance() {
        return INSTANCE;
    }

    /**
     * minimum
     */
    @Override
    public Statistic<Long> minimum() {
        return NullStatistic.instance(null);
    }

    /**
     * maximum
     */
    @Override
    public Statistic<Long> maximum() {
        return NullStatistic.instance(null);
    }

    /**
     * average
     */
    @Override
    public Statistic<Double> average() {
        return NullStatistic.instance(Double.NaN);
    }
}

/**
 * Null statistic class
 * @author cdennis
 *
 * @param <T>
 */
final class NullStatistic<T extends Number> implements Statistic<T> {

    private static final Map<Object, Statistic<?>> COMMON = new HashMap<Object, Statistic<?>>();
    static {
        COMMON.put(Double.NaN, new NullStatistic<Double>(Double.NaN));
        COMMON.put(Float.NaN, new NullStatistic<Float>(Float.NaN));
        COMMON.put(Long.valueOf(0L), new NullStatistic<Long>(0L));
        COMMON.put(null, new NullStatistic(null));
    }

    private final T value;

    /**
     * Constructor
     * @param value initial value
     */
    private NullStatistic(T value) {
        this.value = value;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean active() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T value() {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Timestamped<T>> history() throws UnsupportedOperationException {
        return Collections.emptyList();
    }

    /**
     * instance
     * @param value
     * @return
     */
    static <T extends Number> Statistic<T> instance(T value) {
        Statistic<T> cached = (Statistic<T>) COMMON.get(value);
        if (cached == null) {
            return new NullStatistic<T>(value);
        } else {
            return cached;
        }
    }

}