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
package org.ehcache.management;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Mathieu Carbou
 */
public final class Context {

  private final Map<String, String> back = new LinkedHashMap<String, String>();

  private Context(Map<String, String> back) {
    this.back.putAll(back);
  }

  private Context() {
  }

  public Map<String, String> toMap() {
    return new LinkedHashMap<String, String>(back);
  }

  public Context with(String key, String val) {
    Context context = new Context(back);
    context.back.put(key, val);
    return context;
  }

  public Context with(Map<String, String> props) {
    Context context = new Context(back);
    context.back.putAll(props);
    return context;
  }

  public String get(String key) {
    return back.get(key);
  }

  public int size() {
    return back.size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Context context = (Context) o;
    return back.equals(context.back);
  }

  @Override
  public int hashCode() {
    return back.hashCode();
  }

  public static Context create() {
    return new Context();
  }

  public static Context create(String key, String val) {
    return new Context().with(key, val);
  }

  public static Context create(Map<String, String> map) {
    return new Context(map);
  }

  public static Context empty() {
    return new Context(Collections.<String, String>emptyMap());
  }
}
