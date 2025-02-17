/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

/**
 * Service that offers a {@link TimeSource} to other services needing one.
 */
public interface TimeSourceService extends Service {

  /**
   * Exposes the {@link TimeSource} that is configured
   *
   * @return the {@code TimeSource} to use
   */
  TimeSource getTimeSource();
}
