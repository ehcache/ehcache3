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

import org.ehcache.impl.copy.ReadWriteCopier;

/**
 * Created by alsu on 25/08/15.
 */
public class AnotherPersonCopier extends ReadWriteCopier<Person> {

  @Override
  public Person copy(final Person obj) {
    return new Person(obj.name, obj.age);
  }
}

