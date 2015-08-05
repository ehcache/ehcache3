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
package org.ehcache.jsr107;

import org.ehcache.internal.serialization.JavaSerializer;
import org.ehcache.spi.serialization.DefaultSerializationProvider;

/**
 * @author Ludovic Orban
 */
public class DefaultJsr107SerializationProvider extends DefaultSerializationProvider {

  public DefaultJsr107SerializationProvider() {
    super(null);
  }

  @Override
  protected void addDefaultSerializer() {
    // add java.lang.Object at the end of the map if it wasn't already there
    if (!preconfiguredLoaders.containsKey(Object.class.getName())) {
      preconfiguredLoaders.put(Object.class.getName(), (Class) JavaSerializer.class);
    }
  }

}
