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

package org.ehcache.core.spi.service;

import java.io.File;

/**
 * A file based persistence context as returned by the {@link LocalPersistenceService}.
 */
public interface FileBasedPersistenceContext {

  /**
   * Returns a directory where the user of this persistence context can write its files.
   *
   * @return a directory to use
   */
  File getDirectory();
}
