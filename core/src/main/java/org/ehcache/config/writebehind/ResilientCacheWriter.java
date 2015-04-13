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
package org.ehcache.config.writebehind;


/**
 * This interface allows user to know the failed operations for a particular key.
 * For a loaderwriter to be resilient, the user will have to implement this interface
 * This is required to handle failures during processing in WriteBeindQueue  
 * 
 * @author Abhilash
 *
 */
public interface ResilientCacheWriter<K, V> {
  
  /**
   * This method gets called after all retry attempts to complete the operation
   * are exhausted
   * 
   * @param key Key for which the operation failed
   * @param value for which operation failed , null in case of delete
   * @param e the exception with which the operation failed
   */
  void throwAway(K key, V value, Exception e);

}
