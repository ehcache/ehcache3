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

package org.ehcache.integration;

import org.ehcache.core.spi.time.TimeSource;

/**
 * A manual time source implementation that can be used for testing purposes.
 *
 * @author Albin Suresh
 */
public class TestTimeSource implements TimeSource {

  private long time = 0;

  public TestTimeSource() {
  }

  public TestTimeSource(final long time) {
    this.time = time;
  }

  @Override
  public long getTimeMillis() {
    return time;
  }

  public void setTimeMillis(long time) {
    this.time = time;
  }
}
