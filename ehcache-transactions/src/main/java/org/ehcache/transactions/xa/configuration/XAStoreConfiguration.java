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
package org.ehcache.transactions.xa.configuration;

import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.transactions.xa.internal.XAStore;

/**
 * @author Ludovic Orban
 */
public class XAStoreConfiguration implements ServiceConfiguration<XAStore.Provider, String> {

  private final String uniqueXAResourceId;

  public XAStoreConfiguration(String uniqueXAResourceId) {
    this.uniqueXAResourceId = uniqueXAResourceId;
  }

  public String getUniqueXAResourceId() {
    return uniqueXAResourceId;
  }

  @Override
  public Class<XAStore.Provider> getServiceType() {
    return XAStore.Provider.class;
  }

  @Override
  public String derive() {
    return getUniqueXAResourceId();
  }

  @Override
  public XAStoreConfiguration build(String xaResourceId) {
    return new XAStoreConfiguration(xaResourceId);
  }
}
