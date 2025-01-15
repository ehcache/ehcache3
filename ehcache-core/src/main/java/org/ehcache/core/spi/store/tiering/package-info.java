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

/**
 * Package holding the different SPI interfaces that enable a {@link org.ehcache.Cache} to be backed by multiple
 * {@link org.ehcache.core.spi.store.Store} stacked on each other.
 */
@PublicApi
package org.ehcache.core.spi.store.tiering;

import org.ehcache.javadoc.PublicApi;
