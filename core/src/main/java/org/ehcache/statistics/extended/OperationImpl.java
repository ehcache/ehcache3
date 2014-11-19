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

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.ehcache.statistics.extended.ExtendedStatistics.Latency;
import org.ehcache.statistics.extended.ExtendedStatistics.Result;
import org.ehcache.statistics.extended.ExtendedStatistics.Statistic;

import org.terracotta.statistics.OperationStatistic;

/**
 * The Class OperationImpl.
 *
 * @param <T> the generic type
 * @author cdennis
 */
class OperationImpl<T extends Enum<T>> implements Result {

    /** The source. */
    private final OperationStatistic<T> source;

    /** The count. */
    private final SemiExpiringStatistic<Long> count;

    /** The rate. */
    private final RateImpl rate;

    /** The latency. */
    private final LatencyImpl latency;

    /**
     * Instantiates a new operation impl.
     *
     * @param source the source
     * @param targets the targets
     * @param averageNanos the average nanos
     * @param executor the executor
     * @param historySize the history size
     * @param historyNanos the history nanos
     */
    public OperationImpl(OperationStatistic<T> source, Set<T> targets, long averageNanos,
            ScheduledExecutorService executor, int historySize, long historyNanos) {
        this.source = source;
        this.count = new SemiExpiringStatistic<Long>(source.statistic(targets), executor, historySize, historyNanos);
        this.latency = new LatencyImpl(source, targets, averageNanos, executor, historySize, historyNanos);
        this.rate = new RateImpl(source, targets, averageNanos, executor, historySize, historyNanos);
    }

    /* (non-Javadoc)
     * @see net.sf.ehcache.statisticsV2.extended.ExtendedStatistics.Result#rate()
     */
    @Override
    public Statistic<Double> rate() {
        return rate;
    }

    /* (non-Javadoc)
     * @see net.sf.ehcache.statisticsV2.extended.ExtendedStatistics.Result#latency()
     */
    @Override
    public Latency latency() throws UnsupportedOperationException {
        return latency;
    }

    /* (non-Javadoc)
     * @see net.sf.ehcache.statisticsV2.extended.ExtendedStatistics.Result#count()
     */
    @Override
    public Statistic<Long> count() {
        return count;
    }

    /**
     * Start.
     */
    void start() {
        count.start();
        rate.start();
        latency.start();
    }

    /**
     * Expire.
     *
     * @param expiryTime the expiry time
     * @return true, if successful
     */
    boolean expire(long expiryTime) {
        return (count.expire(expiryTime) && rate.expire(expiryTime) && latency.expire(expiryTime));
    }

    /**
     * Sets the window.
     *
     * @param averageNanos the new window
     */
    void setWindow(long averageNanos) {
        rate.setWindow(averageNanos);
        latency.setWindow(averageNanos);
    }

    /**
     * Sets the history.
     *
     * @param historySize the history size
     * @param historyNanos the history nanos
     */
    void setHistory(int historySize, long historyNanos) {
        count.setHistory(historySize, historyNanos);
        rate.setHistory(historySize, historyNanos);
        latency.setHistory(historySize, historyNanos);
    }
}
