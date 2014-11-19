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
import java.util.concurrent.ScheduledExecutorService;

import org.ehcache.statistics.extended.ExtendedStatistics.Statistic;

import org.terracotta.statistics.ValueStatistic;
import org.terracotta.statistics.archive.Timestamped;

/**
 * The Class AbstractStatistic.
 *
 * @param <T> the generic type
 * @author cdennis
 */
abstract class AbstractStatistic<T extends Number> implements Statistic<T> {

    /** The source. */
    private final ValueStatistic<T> source;
    
    /** The history. */
    private final SampledStatistic<T> history;

    /**
     * Instantiates a new abstract statistic.
     *
     * @param executor the executor
     * @param historySize the history size
     * @param historyNanos the history nanos
     */
    AbstractStatistic(ValueStatistic<T> source, ScheduledExecutorService executor, int historySize, long historyNanos) {
        this.source = source;
        this.history = new SampledStatistic<T>(source, executor, historySize, historyNanos);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T value() {
        return source.value();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Timestamped<T>> history() {
        return history.history();
    }

    /**
     * Start sampling.
     */
    final void startSampling() {
        history.startSampling();
    }

    /**
     * Stop sampling.
     */
    final void stopSampling() {
        history.stopSampling();
    }

    /**
     * Sets the history.
     *
     * @param historySize the history size
     * @param historyNanos the history nanos
     */
    final void setHistory(int historySize, long historyNanos) {
        history.adjust(historySize, historyNanos);
    }
}
