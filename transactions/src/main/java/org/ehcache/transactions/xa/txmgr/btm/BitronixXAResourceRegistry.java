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
package org.ehcache.transactions.xa.txmgr.btm;

import org.ehcache.transactions.xa.txmgr.XAResourceRegistry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.transaction.xa.XAResource;

/**
 * Bitronix's BTM {@link XAResourceRegistry} implementation.
 *
 * @author Ludovic Orban
 */
public class BitronixXAResourceRegistry implements XAResourceRegistry {

  private final ConcurrentMap<String, Ehcache3XAResourceProducer> producers = new ConcurrentHashMap<String, Ehcache3XAResourceProducer>();

  /**
   * Register an XAResource of a cache with BTM. The first time a XAResource is registered a new
   * EhCacheXAResourceProducer is created to hold it.
   * @param uniqueName the uniqueName of this XAResourceProducer, usually the cache's name
   * @param xaResource the XAResource to be registered
   */
  @Override
  public void registerXAResource(String uniqueName, XAResource xaResource) {
    Ehcache3XAResourceProducer xaResourceProducer = producers.get(uniqueName);

    if (xaResourceProducer == null) {
      xaResourceProducer = new Ehcache3XAResourceProducer();
      xaResourceProducer.setUniqueName(uniqueName);
      // the initial xaResource must be added before init() can be called
      xaResourceProducer.addXAResource(xaResource);

      Ehcache3XAResourceProducer previous = producers.putIfAbsent(uniqueName, xaResourceProducer);
      if (previous == null) {
        xaResourceProducer.init();
      } else {
        previous.addXAResource(xaResource);
      }
    } else {
      xaResourceProducer.addXAResource(xaResource);
    }
  }

  /**
   * Unregister an XAResource of a cache from BTM.
   * @param uniqueName the uniqueName of this XAResourceProducer, usually the cache's name
   * @param xaResource the XAResource to be registered
   */
  @Override
  public void unregisterXAResource(String uniqueName, XAResource xaResource) {
    Ehcache3XAResourceProducer xaResourceProducer = producers.get(uniqueName);

    if (xaResourceProducer != null) {
      boolean found = xaResourceProducer.removeXAResource(xaResource);
      if (!found) {
        throw new IllegalStateException("no XAResource " + xaResource + " found in XAResourceProducer with name " + uniqueName);
      }
      if (xaResourceProducer.isEmpty()) {
        xaResourceProducer.close();
        producers.remove(uniqueName);
      }
    } else {
      throw new IllegalStateException("no XAResourceProducer registered with name " + uniqueName);
    }
  }



}
