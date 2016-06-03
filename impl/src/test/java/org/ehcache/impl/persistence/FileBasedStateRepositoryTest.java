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

package org.ehcache.impl.persistence;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * FileBasedStateRepositoryTest
 */
public class FileBasedStateRepositoryTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testMapSave() throws Exception {
    File directory = folder.newFolder("testSave");
    FileBasedStateRepository stateRepository = new FileBasedStateRepository(directory);
    ConcurrentMap<Long, String> myMap = stateRepository.getPersistentConcurrentMap("myMap");

    myMap.put(42L, "TheAnswer!");

    stateRepository.close();

    FileInputStream fis = new FileInputStream(new File(directory, "myMap-map.bin"));
    try {
      ObjectInputStream ois = new ObjectInputStream(fis);
      try {
        ConcurrentMap<Long, String> loadedMap = (ConcurrentMap<Long, String>) ois.readObject();
        assertThat(loadedMap, is(myMap));
      } finally {
        ois.close();
      }
    } finally {
      fis.close();
    }
  }

  @Test
  public void testMapLoad() throws Exception {
    File directory = folder.newFolder("testLoad");
    ConcurrentMap<Long, String> map = new ConcurrentHashMap<Long, String>();
    map.put(42L, "Again? That's not even funny anymore!!");

    FileOutputStream fos = new FileOutputStream(new File(directory, "myMap-map.bin"));
    try {
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      try {
        oos.writeObject(map);
      } finally {
        oos.close();
      }
    } finally {
      fos.close();
    }

    FileBasedStateRepository stateRepository = new FileBasedStateRepository(directory);
    ConcurrentMap<Long, String> myMap = stateRepository.getPersistentConcurrentMap("myMap");

    assertThat(myMap, is(map));
  }
}