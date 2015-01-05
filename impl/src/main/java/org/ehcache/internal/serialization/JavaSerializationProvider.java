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
package org.ehcache.internal.serialization;

import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 *
 * @author cdennis
 */
public class JavaSerializationProvider implements SerializationProvider {

  @Override
  public <T> Serializer<T> createSerializer(Class<T> clazz, ClassLoader classLoader, ServiceConfiguration<?>... config) {
    return new JavaSerializer<T>(classLoader);
  }

  @Override
  public void start(ServiceConfiguration<?> config) {
    //no-op
  }

  @Override
  public void stop() {
    //no-op
  }
}
