/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

package org.ehcache.impl.internal.store.heap.holders;

import org.junit.Test;

import static org.junit.Assert.assertSame;

/**
 * Created by alsu on 20/08/15.
 */
public class SimpleOnHeapValueHolderTest {

  @Test
  public void testValueByRef() throws Exception {
    Person person = new Person("foo", 24);
    SimpleOnHeapValueHolder<Person> valueHolder = new SimpleOnHeapValueHolder<>(person, -1, false);
    person.age = 25;

    assertSame(person, valueHolder.get());
  }

  private static class Person {
    String name;
    int age;

    Person(String name, int age) {
      this.name = name;
      this.age = age;
    }

    @Override
    public boolean equals(final Object other) {
      if(this == other) return true;
      if(other == null || this.getClass() != other.getClass()) return false;

      Person that = (Person)other;
      if(name != that.name || age != that.age) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = 1;
      result = 31 * result + age;
      result = 31 * result + name.hashCode();
      return result;
    }
  }
}
