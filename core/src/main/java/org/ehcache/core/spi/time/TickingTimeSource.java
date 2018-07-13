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

package org.ehcache.core.spi.time;

import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;

import java.util.Timer;
import java.util.TimerTask;

/**
 * A {@link TimeSource} that increases the time in background using a timer. This time usually gives better performances
 * to Ehcache than the default {@link SystemTimeSource}. However, it will create a background thread that will continuously
 * wake up to update the time.
 * <p>
 * It works be increasing the time at a given granularity. So if you set the granularity at 10ms, a timer is called every
 * 10ms and will increase the current time of 10ms. This will cause the current time to diverge a bit from the system time
 * after a while. So you specify a system update period where the current time will be reset to the system time.
 */
public class TickingTimeSource implements TimeSource, Service {

  private final long granularity;
  private final long systemUpdatePeriod;

  private volatile long currentTime;
  private volatile long lastUpdate;

  private final Timer timer = new Timer("Ehcache-TickingTimeSource-timer", true);

  /**
   * Constructor to create a ticking time source.
   *
   * @param granularity how long in milliseconds between each timer call to increment the current time
   * @param systemUpdatePeriod how long between resets of the current time to system time
   */
  public TickingTimeSource(long granularity, long systemUpdatePeriod) {
    this.granularity = granularity;
    this.systemUpdatePeriod = systemUpdatePeriod;
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
        if (currentTime - lastUpdate >= systemUpdatePeriod) {
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
