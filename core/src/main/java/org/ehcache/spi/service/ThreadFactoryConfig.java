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
package org.ehcache.spi.service;

/**
 * @author palmanojkumar
 *
 */
public class ThreadFactoryConfig {
  
  /**
   * no naming format by default.
   */
  private String nameformat = null;
  
  private boolean isDaemon = false;
  
  private int priority = Thread.NORM_PRIORITY;
  
  public String getNameformat() {
    return nameformat;
  }
  
  public void setNameformat(String nameformat) {
    this.nameformat = nameformat;
  }
  
  public boolean isDaemon() {
    return isDaemon;
  }
  
  public void setDaemon(boolean isDaemon) {
    this.isDaemon = isDaemon;
  }
  
  public int getPriority() {
    return priority;
  }
  
  public void setPriority(int priority) {
    this.priority = priority;
  }
  
}
