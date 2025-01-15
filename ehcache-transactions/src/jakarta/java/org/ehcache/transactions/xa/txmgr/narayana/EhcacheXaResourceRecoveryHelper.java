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
package org.ehcache.transactions.xa.txmgr.narayana;

import com.arjuna.ats.jta.recovery.XAResourceRecoveryHelper;

import javax.transaction.xa.XAResource;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Ludovic Orban
 */
class EhcacheXaResourceRecoveryHelper implements XAResourceRecoveryHelper {

  private final Set<XAResource> xaResources = Collections.newSetFromMap(new ConcurrentHashMap<>());

  public EhcacheXaResourceRecoveryHelper(XAResource initialResource) {
    this.xaResources.add(initialResource);
  }

  void addXAResource(XAResource xaResource) {
    xaResources.add(xaResource);
  }

  boolean removeXAResource(XAResource xaResource) {
    return xaResources.remove(xaResource);
  }

  @Override
  public boolean initialise(String p) {
    return true;
  }

  @Override
  public XAResource[] getXAResources() {
    return xaResources.toArray(new XAResource[0]);
  }

  public boolean isEmpty() {
    return xaResources.isEmpty();
  }
}
