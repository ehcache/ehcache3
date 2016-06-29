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

package com.pany.ehcache;

import org.ehcache.config.EvictionAdvisor;

import com.pany.domain.Product;

/**
 * @author Alex Snaps
 */
public class MyEvictionAdvisor implements EvictionAdvisor<Long, Product> {
  @Override
  public boolean adviseAgainstEviction(Long key, Product value) {
    return value.getMutable() != null;
  }
}
