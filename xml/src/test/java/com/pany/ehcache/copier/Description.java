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

package com.pany.ehcache.copier;

/**
 * Created by alsu on 25/08/15.
 */
public class Description {
  int id;
  String alias;

  public Description(Description other) {
    this.id = other.id;
    this.alias = other.alias;
  }

  public Description(int id, String alias) {
    this.id = id;
    this.alias = alias;
  }

  @Override
  public boolean equals(final Object other) {
    if(this == other) return true;
    if(other == null || this.getClass() != other.getClass()) return false;

    Description that = (Description)other;
    if(id != that.id) return false;
    if ((alias == null) ? (alias != null) : !alias.equals(that.alias)) return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + id;
    result = 31 * result + (alias == null ? 0 : alias.hashCode());
    return result;
  }
}
