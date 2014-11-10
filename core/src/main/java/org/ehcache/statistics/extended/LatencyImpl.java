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

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.ehcache.statistics.extended.ExtendedStatistics.Latency;
import org.ehcache.statistics.extended.ExtendedStatistics.Statistic;

import org.terracotta.statistics.SourceStatistic;
import org.terracotta.statistics.Time;
import org.terracotta.statistics.ValueStatistic;
import org.terracotta.statistics.archive.Timestamped;
import org.terracotta.statistics.derived.EventParameterSimpleMovingAverage;
import org.terracotta.statistics.derived.LatencySampling;
import org.terracotta.statistics.observer.ChainedOperationObserver;

/**
 * The Class LatencyImpl.
 *
 * @param <T> the generic type
 * @author cdennis
 */
class LatencyImpl<T extends Enum<T>> implements Latency {
    private final SourceStatistic<ChainedOperationObserver<T>> source;
    private final LatencySampling<T> latencySampler;
    private final EventParameterSimpleMovingAverage average;
    private final StatisticImpl<Long> minimumStatistic;
    private final StatisticImpl<Long> maximumStatistic;
    private final StatisticImpl<Double> averageStatistic;

    private boolean active = false;
    private long touchTimestamp = -1;

    /**
     * Instantiates a new latency impl.
     *
     * @param statistic the statistic
     * @param targets the targets
     * @param averageNanos the average nanos
     * @param executor the executor
     * @param historySize the history size
     * @param historyNanos the history nanos
     */
    public LatencyImpl(SourceStatistic<ChainedOperationObserver<T>> statistic, Set<T> targets, long averageNanos,
            ScheduledExecutorService executor, int historySize, long historyNanos) {
        this.average = new EventParameterSimpleMovingAverage(averageNanos, TimeUnit.NANOSECONDS);
        this.minimumStatistic = new StatisticImpl<Long>(average.minimumStatistic(), executor, historySize, historyNanos);
        this.maximumStatistic = new StatisticImpl<Long>(average.maximumStatistic(), executor, historySize, historyNanos);
        this.averageStatistic = new StatisticImpl<Double>(average.averageStatistic(), executor, historySize, historyNanos);
        this.latencySampler = new LatencySampling(targets, 1.0);
        latencySampler.addDerivedStatistic(average);
        this.source = statistic;
    }

    /**
     * Start.
     */
    synchronized void start() {
        if (!active) {
            source.addDerivedStatistic(latencySampler);
            minimumStatistic.startSampling();
            maximumStatistic.startSampling();
            averageStatistic.startSampling();
            active = true;
        }
    }

    /**
     * Get the minimum
     */
    public Statistic<Long> minimum() {
        return minimumStatistic;
    }

    /**
     * Get the maximum.
     */
    @Override
    public Statistic<Long> maximum() {
        return maximumStatistic;
    }

    /**
     * Get the average.
     */
    @Override
    public Statistic<Double> average() {
        return averageStatistic;
    }

    private synchronized void touch() {
        touchTimestamp = Time.absoluteTime();
        start();
    }

    /**
     * Expire.
     *
     * @param expiry the expiry
     * @return true, if successful
     */
    public synchronized boolean expire(long expiry) {
        if (touchTimestamp < expiry) {
            if (active) {
                source.removeDerivedStatistic(latencySampler);
                minimumStatistic.stopSampling();
                maximumStatistic.stopSampling();
                averageStatistic.stopSampling();
                active = false;
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Sets the window.
     *
     * @param averageNanos the new window
     */
    void setWindow(long averageNanos) {
        average.setWindow(averageNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Sets the history.
     *
     * @param historySize the history size
     * @param historyNanos the history nanos
     */
    void setHistory(int historySize, long historyNanos) {
        minimumStatistic.setHistory(historySize, historyNanos);
        maximumStatistic.setHistory(historySize, historyNanos);
        averageStatistic.setHistory(historySize, historyNanos);
    }

    /**
     * The Class StatisticImpl.
     *
     * @param <T> the generic type
     */
    class StatisticImpl<T extends Number> extends AbstractStatistic<T> {

        /**
         * Instantiates a new statistic impl.
         *
         * @param value the value
         * @param executor the executor
         * @param historySize the history size
         * @param historyNanos the history nanos
         */
        public StatisticImpl(ValueStatistic<T> value, ScheduledExecutorService executor, int historySize, long historyNanos) {
            super(value, executor, historySize, historyNanos);
        }

        /*
         * (non-Javadoc)
         *
         * @see net.sf.ehcache.statistics.extended.ExtendedStatistics.Statistic#active()
         */
        @Override
        public boolean active() {
            return active;
        }

        /*
         * (non-Javadoc)
         *
         * @see net.sf.ehcache.statistics.extended.ExtendedStatistics.Statistic#value()
         */
        @Override
        public T value() {
            touch();
            return super.value();
        }

        /*
         * (non-Javadoc)
         *
         * @see net.sf.ehcache.statistics.extended.ExtendedStatistics.Statistic#history()
         */
        @Override
        public List<Timestamped<T>> history() throws UnsupportedOperationException {
            touch();
            return super.history();
        }
    }
}
