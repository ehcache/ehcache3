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
package org.ehcache.xml.ss;

import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.util.Collections;
import java.util.List;

public class SimpleServiceConfiguration implements ServiceCreationConfiguration<SimpleServiceProvider, Void> {
  private final Class<? extends Service> clazz;
  private final List<String> unparsedArgs;

  public SimpleServiceConfiguration(Class<? extends Service> clazz, List<String> unparsedArgs) {
    this.clazz = clazz;
    this.unparsedArgs = Collections.unmodifiableList(unparsedArgs);
  }

  public Class<? extends Service> getClazz() {
    return clazz;
  }

  public List<String> getUnparsedArgs() {
    return unparsedArgs;
  }

  @Override
  public Class<SimpleServiceProvider> getServiceType() {
    return SimpleServiceProvider.class;
  }
}
