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
package org.ehcache.docs.plugs;

import org.ehcache.spi.copy.Copier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ludovic Orban
 */
public class LongCopier implements Copier<Long> {
  private static final Logger LOG = LoggerFactory.getLogger(LongCopier.class);

  @Override
  public Long copyForRead(Long obj) {
    LOG.info("Copying for read {}", obj);
    return obj;
  }

  @Override
  public Long copyForWrite(Long obj) {
    LOG.info("Copying for write {}", obj);
    return obj;
  }
}
