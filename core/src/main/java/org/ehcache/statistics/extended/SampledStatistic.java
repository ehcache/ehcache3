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
import java.util.concurrent.TimeUnit;
import org.terracotta.statistics.ValueStatistic;
import org.terracotta.statistics.archive.StatisticArchive;
import org.terracotta.statistics.archive.StatisticSampler;
import org.terracotta.statistics.archive.Timestamped;

/**
 * The Class SampledStatistic.
 *
 * @param <T> the generic type
 * @author cdennis
 */
class SampledStatistic<T extends Number> {

    /** The sampler. */
    private final StatisticSampler<T> sampler;

    /** The history. */
    private final StatisticArchive<T> history;

    /**
     * Instantiates a new sampled statistic.
     *
     * @param statistic the statistic
     * @param executor the executor
     * @param historySize the history size
     * @param periodNanos the period nanos
     */
    public SampledStatistic(ValueStatistic<T> statistic, ScheduledExecutorService executor, int historySize, long periodNanos) {
        this.history = new StatisticArchive<T>(historySize);
        this.sampler = new StatisticSampler<T>(executor, periodNanos, TimeUnit.NANOSECONDS, statistic, history);
    }

    /**
     * Start sampling.
     */
    public void startSampling() {
        sampler.start();
    }

    /**
     * Stop sampling.
     */
    public void stopSampling() {
        sampler.stop();
        history.clear();
    }

    /**
     * History.
     *
     * @return the list
     */
    public List<Timestamped<T>> history() {
        return history.getArchive();
    }

    /**
     * Adjust.
     *
     * @param historySize the history size
     * @param historyNanos the history nanos
     */
    void adjust(int historySize, long historyNanos) {
        history.setCapacity(historySize);
        sampler.setPeriod(historyNanos, TimeUnit.NANOSECONDS);
    }
}
