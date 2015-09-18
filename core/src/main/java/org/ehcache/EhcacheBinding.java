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
package org.ehcache;

/**
 * Class representing an association between an object and an alias, name, identifier
 *
 * @author Mathieu Carbou
 */
public final class EhcacheBinding {

  private final String alias;
  private final Ehcache<?, ?> value;

  public EhcacheBinding(String alias, Ehcache<?, ?> value) {
    if (alias == null) throw new NullPointerException();
    if (value == null) throw new NullPointerException();
    this.alias = alias;
    this.value = value;
  }

  public String getAlias() {
    return alias;
  }

  public Ehcache<?, ?> getCache() {
    return value;
  }

}
