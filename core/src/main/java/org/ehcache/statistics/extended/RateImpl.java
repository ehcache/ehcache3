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

import org.ehcache.statistics.extended.ExtendedStatistics.Statistic;
import org.terracotta.statistics.SourceStatistic;
import org.terracotta.statistics.archive.Timestamped;
import org.terracotta.statistics.derived.EventRateSimpleMovingAverage;
import org.terracotta.statistics.derived.OperationResultFilter;
import org.terracotta.statistics.observer.ChainedOperationObserver;

/**
 * The Class RateStatistic.
 *
 * @param <T> the generic type
 * @author cdennis
 */
public class RateImpl<T extends Enum<T>> implements Statistic<Double> {

    private final ExpiringStatistic<Double> delegate;
    
    /** The rate. */
    private final EventRateSimpleMovingAverage rate;

    /**
     * Instantiates a new rate statistic.
     *
     * @param targets the targets
     * @param averageNanos the average nanos
     * @param executor the executor
     * @param historySize the history size
     * @param historyNanos the history nanos
     */
    public RateImpl(final SourceStatistic<ChainedOperationObserver<T>> source, final Set<T> targets, long averageNanos,
            ScheduledExecutorService executor, int historySize, long historyNanos) {
        this.rate = new EventRateSimpleMovingAverage(averageNanos, TimeUnit.NANOSECONDS);
        this.delegate = new ExpiringStatistic<Double>(rate, executor, historySize, historyNanos) {

          private final ChainedOperationObserver<T> observer = new OperationResultFilter<T>(targets, rate);
          
          @Override
          protected void stopStatistic() {
              super.stopStatistic();
              source.removeDerivedStatistic(observer);
          }

          @Override
          protected void startStatistic() {
              super.startStatistic();
              source.addDerivedStatistic(observer);
          }
        };
    }

    @Override
    public boolean active() {
        return delegate.active();
    }

    @Override
    public Double value() {
        return delegate.value();
    }

    @Override
    public List<Timestamped<Double>> history() {
        return delegate.history();
    }
    
    /**
     * Start sampling.
     */
    protected void start() {
        delegate.start();
    }

    /**
     * Sets the window.
     *
     * @param averageNanos the new window
     */
    protected void setWindow(long averageNanos) {
        rate.setWindow(averageNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Set the sample history parameters.
     * 
     * @param historySize history sample size
     * @param historyNanos history sample period
     */
    protected void setHistory(int historySize, long historyNanos) {
        delegate.setHistory(historySize, historyNanos);
    }

    /**
     * Check the statistic for expiry.
     * 
     * @param expiry expiry threshold
     * @return {@code true} if expired
     */
    protected boolean expire(long expiry) {
      return delegate.expire(expiry);
    }
}
