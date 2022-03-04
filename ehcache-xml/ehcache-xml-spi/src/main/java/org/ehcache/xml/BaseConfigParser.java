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
package org.ehcache.xml;

import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.net.URI;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * BaseConfigParser - Base class providing functionality for translating service configurations to corresponding xml
 * document.
 */
public abstract class BaseConfigParser<T> implements Parser<T> {

  private final Class<T> typeParameterClass;
  private final Map<URI, URL> namespaces;

  @SuppressWarnings("unchecked")
  public BaseConfigParser(Map<URI, URL> namespaces) {
    this.typeParameterClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    this.namespaces = unmodifiableMap(new LinkedHashMap<>(namespaces));
    this.namespaces.keySet().forEach(Objects::requireNonNull);
    this.namespaces.values().forEach(Objects::requireNonNull);
  }

  @Override
  public Map<URI, Supplier<Source>> getSchema() {
    return namespaces.entrySet().stream().collect(toLinkedHashMap(Map.Entry::getKey, e -> () -> {
      try {
        return new StreamSource(e.getValue().openStream());
      } catch (IOException ex) {
        throw new XmlConfigurationException(ex);
      }
    }));
  }

  private T validateConfig(Object config) {
    try {
      return typeParameterClass.cast(requireNonNull(config, "Configuration must not be null."));
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Invalid configuration parameter passed.", e);
    }
  }

  public final Element unparse(Document document, T config) {
    return safeUnparse(document, validateConfig(config));
  }

  protected abstract Element safeUnparse(Document doc, T config);

  protected static <T, K, V> Collector<T, ?, LinkedHashMap<K, V>> toLinkedHashMap(
    Function<? super T, ? extends K> keyMapper,
    Function<? super T, ? extends V> valueMapper
  ) {
    return toMap(keyMapper, valueMapper,(u,v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); }, LinkedHashMap::new);
  }
}
