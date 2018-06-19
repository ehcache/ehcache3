/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.core.spi.time;

import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;

import java.util.Timer;
import java.util.TimerTask;

/**
 * The default {@link TimeSource} in Ehcache. it increases the time every millisecond
 * using a timer. We set it to the system time every second to prevent time drifting.
 */
public class TickingTimeSource implements TimeSource, Service {

  private static final long GRANULARITY_IN_MS = 1L;

  private static final long PERIOD_BETWEEN_SYSTEM_SET_IN_MS = 1_000L;

  private final long granularity;
  private final long period;

  private volatile long currentTime;
  private volatile long lastUpdate;

  private final Timer timer = new Timer("TickingTimeSource-timer", true);

  public TickingTimeSource() {
    this(GRANULARITY_IN_MS, PERIOD_BETWEEN_SYSTEM_SET_IN_MS);
  }

  public TickingTimeSource(long granularity, long period) {
    this.granularity = granularity;
    this.period = period;
  }

  private void updateToSystemTime() {
    long time = System.currentTimeMillis();
    currentTime = time;
    lastUpdate = time;
  }

  @Override
  public long getTimeMillis() {
    return currentTime;
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    updateToSystemTime();
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        if (currentTime - lastUpdate >= period) {
          updateToSystemTime();
        } else {
          currentTime += granularity;
        }
      }
    }, granularity, granularity);
  }

  @Override
  public void stop() {
    timer.cancel();
    timer.purge();
  }
}
