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
import java.io.FilenameFilter;
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

  private static String MAP_FILE_NAME = "map-0-myMap.bin";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testMapSave() throws Exception {
    File directory = folder.newFolder("testSave");
    FileBasedStateRepository stateRepository = new FileBasedStateRepository(directory);
    String mapName = "myMap";
    ConcurrentMap<Long, String> myMap = stateRepository.getPersistentConcurrentMap(mapName, Long.class, String.class);

    myMap.put(42L, "TheAnswer!");

    stateRepository.close();

    FileInputStream fis = new FileInputStream(new File(directory, MAP_FILE_NAME));
    try {
      ObjectInputStream ois = new ObjectInputStream(fis);
      try {
        String name = (String) ois.readObject();
        assertThat(name, is(mapName));
        FileBasedStateRepository.Tuple loadedTuple = (FileBasedStateRepository.Tuple) ois.readObject();
        assertThat(loadedTuple.index, is(0));
        assertThat((ConcurrentMap<Long, String>)loadedTuple.map, is(myMap));
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
    String mapName = "myMap";
    ConcurrentMap<Long, String> map = new ConcurrentHashMap<Long, String>();
    map.put(42L, "Again? That's not even funny anymore!!");

    FileOutputStream fos = new FileOutputStream(new File(directory, MAP_FILE_NAME));
    try {
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      try {
        oos.writeObject(mapName);
        oos.writeObject(new FileBasedStateRepository.Tuple(0, map));
      } finally {
        oos.close();
      }
    } finally {
      fos.close();
    }

    FileBasedStateRepository stateRepository = new FileBasedStateRepository(directory);
    ConcurrentMap<Long, String> myMap = stateRepository.getPersistentConcurrentMap(mapName, Long.class, String.class);

    assertThat(myMap, is(map));
  }

  @Test
  public void testIndexProperlySetAfterLoad() throws Exception {
    File directory = folder.newFolder("testIndexAfterLoad");
    String mapName = "myMap";

    FileOutputStream fos = new FileOutputStream(new File(directory, MAP_FILE_NAME));
    try {
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      try {
        oos.writeObject(mapName);
        oos.writeObject(new FileBasedStateRepository.Tuple(0, new ConcurrentHashMap<Long, String>()));
      } finally {
        oos.close();
      }
    } finally {
      fos.close();
    }

    FileBasedStateRepository stateRepository = new FileBasedStateRepository(directory);
    stateRepository.getPersistentConcurrentMap("otherMap", Long.class, Long.class);
    stateRepository.close();

    File[] files = directory.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.contains("otherMap") && name.contains("-1-");
      }
    });

    assertThat(files.length, is(1));
  }
}