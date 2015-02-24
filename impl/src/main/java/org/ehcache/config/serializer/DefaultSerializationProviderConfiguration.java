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

import java.util.HashMap;
import java.util.Map;

import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.spi.service.ServiceConfiguration;

public class DefaultSerializationProviderConfiguration implements ServiceConfiguration<JavaSerializationProvider> {

  private final Map<String, TypeSerializerConfig> serializerConfig = new HashMap<String, TypeSerializerConfig>();
  
  @Override
  public Class<JavaSerializationProvider> getServiceType() {
    return JavaSerializationProvider.class;
  }
  
  public DefaultSerializationProviderConfiguration addSerializer(String type , TypeSerializerConfig typeSerializerConfig){
    serializerConfig.put(type, typeSerializerConfig);
    return this;
  }
  
  public boolean contains(String type){
    return serializerConfig.containsKey(type);
  }
  
  public TypeSerializerConfig getTypeSerializerConfig(String type){
    return serializerConfig.get(type);
  }
  
  public static class TypeSerializerConfig {
    private String serializer;
    private final Map<String, String> cacheTypeSerializerMapping = new HashMap<String, String>();
       
    public String getSerializer() {
      return serializer;
    }
    
    public void setSerializer(String serializer) {
      this.serializer = serializer;
    }

    public Map<String, String> getCacheTypeSerializerMapping() {
      return cacheTypeSerializerMapping;
    }
    
    public void addTypeSerializerMapping(String alias, String typeSerializer) {
      cacheTypeSerializerMapping.put(alias, typeSerializer);
    }
    
  }

}
