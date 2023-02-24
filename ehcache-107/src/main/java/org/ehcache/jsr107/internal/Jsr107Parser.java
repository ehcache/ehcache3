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

package org.ehcache.jsr107.internal;

import org.ehcache.xml.spi.BaseConfigParser;

import java.net.URI;

import static java.util.Collections.singletonMap;

public abstract class Jsr107Parser<T> extends BaseConfigParser<T> {

  private static final URI NAMESPACE = URI.create("http://www.ehcache.org/v3/jsr107");

  public Jsr107Parser() {
    super(singletonMap(NAMESPACE, Jsr107Parser.class.getResource("/ehcache-107-ext.xsd")));
  }
}
