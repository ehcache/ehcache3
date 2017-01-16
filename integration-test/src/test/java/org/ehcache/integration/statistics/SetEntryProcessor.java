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
package org.ehcache.integration.statistics;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;

public class SetEntryProcessor implements EntryProcessor<Integer, String, String> {

  private String value;

  public SetEntryProcessor(String value) {
    this.value = value;
  }

  @Override
  public String process(MutableEntry<Integer, String> entry, Object... arguments) {
    entry.setValue(value);
    return value;
  }

}
