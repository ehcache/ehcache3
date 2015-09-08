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

package org.ehcache.config.serializer;

import org.ehcache.config.SerializerConfiguration;
import org.ehcache.internal.classes.ClassInstanceConfiguration;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;

/**
 * @author Ludovic Orban
 */
public class ExtendedSerializerConfiguration<T> implements SerializerConfiguration<SerializationProvider> {

  private final Type type;
  private final String qualifier;

  public ExtendedSerializerConfiguration(Type type, String qualifier) {
    this.type = type;
    this.qualifier = qualifier;
  }

  @Override
  public Class<SerializationProvider> getServiceType() {
    return SerializationProvider.class;
  }

  public Type getType() {
    return type;
  }

  public String getQualifier() {
    return qualifier;
  }
}
