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
package org.ehcache.management.providers;

import org.ehcache.EhcacheManager;
import org.terracotta.management.capabilities.context.CapabilityContext;
import org.terracotta.management.context.ContextContainer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class CapabilityContextProvider extends AbstractActionProvider<EhcacheManager, EhcacheManagerContext> {

  @Override
  protected EhcacheManagerContext createActionWrapper(EhcacheManager ehcacheManager) {
    return new EhcacheManagerContext(ehcacheManager);
  }

  @Override
  public Class<EhcacheManager> managedType() {
    return EhcacheManager.class;
  }

  @Override
  public CapabilityContext capabilityContext() {
    return new CapabilityContext(Collections.<CapabilityContext.Attribute>emptySet());
  }

  @Override
  public Object callAction(Map<String, String> context, String methodName, String[] argClassNames, Object[] args) {
    throw new UnsupportedOperationException("Cannot be called directly");
  }

  public Collection<ContextContainer> contexts() {
    Collection<ContextContainer> result = new ArrayList<ContextContainer>();
    for (EhcacheManager ehcacheManager : actions.keySet()) {
      result.add(context(ehcacheManager));
    }
    return result;
  }

  private ContextContainer context(EhcacheManager ehcacheManager) {
    EhcacheManagerContext ehcacheManagerContext = actions.get(ehcacheManager);

    Collection<ContextContainer> subContextContainers = new ArrayList<ContextContainer>();
    for (String cacheName : ehcacheManagerContext.cacheNames()) {
      subContextContainers.add(new ContextContainer("cacheName", cacheName, null));
    }

    return new ContextContainer("cacheManagerName", ehcacheManagerContext.cacheManagerName(), subContextContainers);
  }

}
