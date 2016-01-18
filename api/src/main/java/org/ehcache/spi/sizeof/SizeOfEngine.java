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
package org.ehcache.spi.sizeof;

/**
 * SizeOf engines are used to calculate the size of objects.
 * 
 * @author Abhilash
 *
 */
public interface SizeOfEngine {
  
  /**
   * Size of the object on Heap
   * 
   * @param objects objects to be sized
   * @return size of the objects on heap 
   */
  long sizeof(Object... objects);
  
  /**
   * Sizes the key alongwith the offset of 
   * the key holder
   * 
   * @param key key to be sized
   * @return size of the key alongwith the offset
   */
  long sizeofKey(Object key);
  
  /**
   * Sizes the key alongwith the offset of 
   * the value holder 
   * 
   * @param value value to be sized
   * @return size of the key alongwith the offset
   */
  long sizeofValue(Object value);
  
  /**
   * bytes required on adding each key-value pair
   * 
   * @return size overhead of adding a key-valu pair
   */
  long getCHMOffset();
  

}
