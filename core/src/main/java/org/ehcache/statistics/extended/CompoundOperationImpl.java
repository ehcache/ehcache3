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

import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.ehcache.statistics.extended.ExtendedStatistics.Operation;
import org.ehcache.statistics.extended.ExtendedStatistics.Result;
import org.ehcache.statistics.extended.ExtendedStatistics.Statistic;

import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.ValueStatistic;

/**
 * The Class CompoundOperationImpl.
 *
 * @param <T> the generic type
 * @author cdennis
 */
class CompoundOperationImpl<T extends Enum<T>> implements Operation<T> {

    private final OperationStatistic<T> source;

    private final Class<T> type;
    private final Map<T, OperationImpl<T>> operations;
    private final ConcurrentMap<Set<T>, OperationImpl<T>> compounds = new ConcurrentHashMap<Set<T>, OperationImpl<T>>();
    private final ConcurrentMap<List<Set<T>>, ExpiringStatistic<Double>> ratios = new ConcurrentHashMap<List<Set<T>>, ExpiringStatistic<Double>>();

    private final ScheduledExecutorService executor;

    private volatile long averageNanos;
    private volatile int historySize;
    private volatile long historyNanos;

    private volatile boolean alwaysOn = false;

    /**
     * Instantiates a new compound operation impl.
     *
     * @param source the source
     * @param type the type
     * @param averagePeriod the average period
     * @param averageUnit the average unit
     * @param executor the executor
     * @param historySize the history size
     * @param historyPeriod the history period
     * @param historyUnit the history unit
     */
    public CompoundOperationImpl(OperationStatistic<T> source, Class<T> type, long averagePeriod, TimeUnit averageUnit,
            ScheduledExecutorService executor, int historySize, long historyPeriod, TimeUnit historyUnit) {
        this.type = type;
        this.source = source;

        this.averageNanos = averageUnit.toNanos(averagePeriod);
        this.executor = executor;
        this.historySize = historySize;
        this.historyNanos = historyUnit.toNanos(historyPeriod);

        this.operations = new EnumMap(type);
        for (T result : type.getEnumConstants()) {
            operations.put(result, new OperationImpl(source, EnumSet.of(result), averageNanos, executor, historySize, historyNanos));
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see net.sf.ehcache.statistics.extended.ExtendedStatistics.Operation#type()
     */
    @Override
    public Class<T> type() {
        return type;
    }

    /*
     * (non-Javadoc)
     *
     * @see net.sf.ehcache.statistics.extended.ExtendedStatistics.Operation#component(java.lang.Enum)
     */
    @Override
    public Result component(T result) {
        return operations.get(result);
    }

    /*
     * (non-Javadoc)
     *
     * @see net.sf.ehcache.statistics.extended.ExtendedStatistics.Operation#compound(java.util.Set)
     */
    @Override
    public Result compound(Set<T> results) {
        if (results.size() == 1) {
            return component(results.iterator().next());
        } else {
            Set<T> key = EnumSet.copyOf(results);
            OperationImpl<T> existing = compounds.get(key);
            if (existing == null) {
                OperationImpl<T> created = new OperationImpl(source, key, averageNanos, executor, historySize, historyNanos);
                OperationImpl<T> racer = compounds.putIfAbsent(key, created);
                if (racer == null) {
                    return created;
                } else {
                    return racer;
                }
            } else {
                return existing;
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see net.sf.ehcache.statistics.extended.ExtendedStatistics.Operation#ratioOf(java.util.Set, java.util.Set)
     */
    @Override
    public Statistic<Double> ratioOf(Set<T> numerator, Set<T> denominator) {
        List<Set<T>> key = Arrays.<Set<T>> asList(EnumSet.copyOf(numerator), EnumSet.copyOf(denominator));
        ExpiringStatistic<Double> existing = ratios.get(key);
        if (existing == null) {
            final Statistic<Double> numeratorRate = compound(numerator).rate();
            final Statistic<Double> denominatorRate = compound(denominator).rate();
            ExpiringStatistic<Double> created = new ExpiringStatistic(new ValueStatistic<Double>() {
                @Override
                public Double value() {
                    return numeratorRate.value() / denominatorRate.value();
                }
            }, executor, historySize, historyNanos);
            ExpiringStatistic<Double> racer = ratios.putIfAbsent(key, created);
            if (racer == null) {
                return created;
            } else {
                return racer;
            }
        } else {
            return existing;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see net.sf.ehcache.statistics.extended.ExtendedStatistics.Operation#setAlwaysOn(boolean)
     */
    @Override
    public void setAlwaysOn(boolean enable) {
        alwaysOn = enable;
        if (enable) {
            for (OperationImpl<T> op : operations.values()) {
                op.start();
            }
            for (OperationImpl<T> op : compounds.values()) {
                op.start();
            }
            for (ExpiringStatistic<Double> ratio : ratios.values()) {
                ratio.start();
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see net.sf.ehcache.statistics.extended.ExtendedStatistics.Operation#isAlwaysOn()
     */
    @Override
    public boolean isAlwaysOn() {
        return alwaysOn;
    }

    /*
     * (non-Javadoc)
     *
     * @see net.sf.ehcache.statistics.extended.ExtendedStatistics.Operation#setWindow(long, java.util.concurrent.TimeUnit)
     */
    @Override
    public void setWindow(long time, TimeUnit unit) {
        averageNanos = unit.toNanos(time);
        for (OperationImpl<T> op : operations.values()) {
            op.setWindow(averageNanos);
        }
        for (OperationImpl<T> op : compounds.values()) {
            op.setWindow(averageNanos);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see net.sf.ehcache.statistics.extended.ExtendedStatistics.Operation#setHistory(int, long, java.util.concurrent.TimeUnit)
     */
    @Override
    public void setHistory(int samples, long time, TimeUnit unit) {
        historySize = samples;
        historyNanos = unit.toNanos(time);
        for (OperationImpl<T> op : operations.values()) {
            op.setHistory(historySize, historyNanos);
        }
        for (OperationImpl<T> op : compounds.values()) {
            op.setHistory(historySize, historyNanos);
        }
        for (ExpiringStatistic<Double> ratio : ratios.values()) {
            ratio.setHistory(historySize, historyNanos);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see net.sf.ehcache.statistics.extended.ExtendedStatistics.Operation#getWindowSize(java.util.concurrent.TimeUnit)
     */
    @Override
    public long getWindowSize(TimeUnit unit) {
        return unit.convert(averageNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Get the history sample size.
     */
    @Override
    public int getHistorySampleSize() {
        return historySize;
    }

    /**
     * Get the history sample time.
     */
    @Override
    public long getHistorySampleTime(TimeUnit unit) {
        return unit.convert(historySize, TimeUnit.NANOSECONDS);
    }

    /**
     * Expire.
     *
     * @param expiryTime the expiry time
     * @return true, if successful
     */
    boolean expire(long expiryTime) {
        if (alwaysOn) {
            return false;
        } else {
            boolean expired = true;
            for (OperationImpl<?> o : operations.values()) {
                expired &= o.expire(expiryTime);
            }
            for (Iterator<OperationImpl<T>> it = compounds.values().iterator(); it.hasNext();) {
                if (it.next().expire(expiryTime)) {
                    it.remove();
                }
            }
            for (Iterator<ExpiringStatistic<Double>> it = ratios.values().iterator(); it.hasNext();) {
                if (it.next().expire(expiryTime)) {
                    it.remove();
                }
            }
            return expired & compounds.isEmpty() & ratios.isEmpty();
        }
    }

}
