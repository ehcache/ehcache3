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
package org.ehcache.impl.internal.util;

import org.ehcache.core.spi.time.TimeSource;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Class used to pace a call to prevent it from happening too frequently.
 */
public class Pacer {

  private final AtomicLong nextLogTime;
  private final TimeSource timeSource;
  private final long delay;

  /**
   * Unique constructor
   *
   * @param delay delay between each call paced
   */
  public Pacer(TimeSource timeSource, long delay) {
    this.timeSource = timeSource;
    this.delay = delay;
    this.nextLogTime = new AtomicLong(timeSource.getTimeMillis());
  }

  /**
   * Execute the call at the request page or call the alternative the rest of the time. An example would be to log
   * a repetitive error once every 30 seconds or always if in debug.
   * <p>
   * <pre>{@code
   * Pacer pacer = new Pacer(30_000);
   * String errorMessage = "my error";
   * pacer.pacedCall(() -> log.error(errorMessage), () -> log.debug(errorMessage);
   * }
   * </pre>
   *
   * @param call call to be paced
   * @param orElse call to be done everytime
   */
  public void pacedCall(Runnable call, Runnable orElse) {
    long now = timeSource.getTimeMillis();
    long end = nextLogTime.get();
    if(now >= end && nextLogTime.compareAndSet(end, now + delay)) {
      call.run();
    } else {
      orElse.run();
    }
  }
}
