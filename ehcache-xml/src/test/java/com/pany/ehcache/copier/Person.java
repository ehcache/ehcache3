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
public class Person {
  String name;
  int age;

  public Person(Person other) {
    this.name = other.name;
    this.age = other.age;
  }

  public Person(String name, int age) {
    this.name = name;
    this.age = age;
  }

  @Override
  public boolean equals(final Object other) {
    if(this == other) return true;
    if(other == null || this.getClass() != other.getClass()) return false;

    Person that = (Person)other;
    if(age != that.age) return false;
    if((name == null) ? (that.name != null) : !name.equals(that.name)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + age;
    result = 31 * result + (name == null ? 0 : name.hashCode());
    return result;
  }
}
