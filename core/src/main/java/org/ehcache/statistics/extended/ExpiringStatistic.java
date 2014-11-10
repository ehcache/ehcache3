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

import java.util.concurrent.ScheduledExecutorService;

import org.terracotta.statistics.ValueStatistic;

/**
 * Statistic that stops sampling history when the last access is after a user supplied timestamp.
 *
 * @param <T> statistic type
 * @author Chris Dennis
 */
class ExpiringStatistic<T extends Number> extends SemiExpiringStatistic<T> {

    /**
     * Creates an expiring statistic.
     * 
     * @param source statistic source
     * @param executor executor to use for sampling
     * @param historySize size of sample history
     * @param historyNanos period between samples
     */
    public ExpiringStatistic(ValueStatistic<T> source, ScheduledExecutorService executor, int historySize, long historyNanos) {
        super(source, executor, historySize, historyNanos);
    }

    @Override
    public T value() {
        touch();
        return super.value();
    }
}
