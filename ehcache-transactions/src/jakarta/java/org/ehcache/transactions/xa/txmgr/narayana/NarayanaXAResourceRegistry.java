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
package org.ehcache.transactions.xa.txmgr.narayana;

import com.arjuna.ats.internal.jta.recovery.arjunacore.XARecoveryModule;
import org.ehcache.transactions.xa.txmgr.XAResourceRegistry;

import javax.transaction.xa.XAResource;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NarayanaXAResourceRegistry implements XAResourceRegistry {

  private final ConcurrentMap<String, EhcacheXaResourceRecoveryHelper> recoveryHelpers = new ConcurrentHashMap<>();

  /**
   * Register an XAResource of a cache with the Narayana recovery module.
   * The first time an XAResource is registered a new EhcacheXaResourceRecoveryHelper is created to hold it.
   * @param uniqueName the uniqueName of this EhcacheXaResourceRecoveryHelper, usually the cache's name
   * @param xaResource the XAResource to be registered
   */
  @Override
  public void registerXAResource(String uniqueName, XAResource xaResource) {
    EhcacheXaResourceRecoveryHelper recoveryHelper = recoveryHelpers.get(uniqueName);

    if (recoveryHelper == null) {
      recoveryHelper = new EhcacheXaResourceRecoveryHelper(xaResource);

      EhcacheXaResourceRecoveryHelper previous = recoveryHelpers.putIfAbsent(uniqueName, recoveryHelper);
      if (previous == null) {
        XARecoveryModule.getRegisteredXARecoveryModule().addXAResourceRecoveryHelper(recoveryHelper);
      } else {
        previous.addXAResource(xaResource);
      }
    } else {
      recoveryHelper.addXAResource(xaResource);
    }
  }

  /**
   * Unregister an XAResource of a cache from BTM.
   * @param uniqueName the uniqueName of this XAResourceProducer, usually the cache's name
   * @param xaResource the XAResource to be registered
   */
  @Override
  public void unregisterXAResource(String uniqueName, XAResource xaResource) {
    EhcacheXaResourceRecoveryHelper recoveryHelper = recoveryHelpers.get(uniqueName);

    if (recoveryHelper != null) {
      boolean found = recoveryHelper.removeXAResource(xaResource);
      if (!found) {
        throw new IllegalStateException("no XAResource " + xaResource + " found in EhcacheXaResourceRecoveryHelper with name " + uniqueName);
      }
      if (recoveryHelper.isEmpty()) {
        //TODO :: this is racy
        XARecoveryModule.getRegisteredXARecoveryModule().removeXAResourceRecoveryHelper(recoveryHelpers.remove(uniqueName));
      }
    } else {
      throw new IllegalStateException("no XAResourceProducer registered with name " + uniqueName);
    }
  }



}
