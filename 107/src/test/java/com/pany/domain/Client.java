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

package com.pany.domain;

import java.io.Serializable;

/**
 * Client
 */
public class Client implements Serializable {

  private final String name;
  private final long creditLine;

  public Client(String name, long creditLine) {
    this.name = name;
    this.creditLine = creditLine;
  }

  public Client(Client toCopy) {
    this.name = toCopy.getName();
    this.creditLine = toCopy.getCreditLine();
  }

  public String getName() {
    return name;
  }

  public long getCreditLine() {
    return creditLine;
  }
}
