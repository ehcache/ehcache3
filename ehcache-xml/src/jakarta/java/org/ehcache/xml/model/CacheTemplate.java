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

package org.ehcache.xml.model;

import org.w3c.dom.Element;

import java.util.Collection;
import java.util.List;

public interface CacheTemplate {

  String id();

  String keyType();

  String keySerializer();

  String keyCopier();

  String valueType();

  String valueSerializer();

  String valueCopier();

  String evictionAdvisor();

  Expiry expiry();

  Heap getHeap();

  List<Element> getResources();

  String loaderWriter();

  String resilienceStrategy();

  ListenersConfig listenersConfig();

  Collection<Element> serviceConfigExtensions();

  CacheLoaderWriterType.WriteBehind writeBehind();

  DiskStoreSettingsType diskStoreSettings();

  SizeOfEngineLimits heapStoreSettings();

  class Impl extends CacheSpec {

    public Impl(CacheTemplateType cacheTemplateType) {
      super(cacheTemplateType.getName(), cacheTemplateType);
    }
  }

}
