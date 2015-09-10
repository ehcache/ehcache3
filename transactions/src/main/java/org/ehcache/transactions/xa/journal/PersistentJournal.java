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
package org.ehcache.transactions.xa.journal;

import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.transactions.xa.TransactionId;
import org.ehcache.transactions.xa.XAState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class PersistentJournal extends TransientJournal {

  private static final Logger LOGGER = LoggerFactory.getLogger(PersistentJournal.class);
  private static final String JOURNAL_FILENAME = "journal.data";

  private final ConcurrentHashMap<TransactionId, XAState> states = new ConcurrentHashMap<TransactionId, XAState>();
  private final File directory;

  public PersistentJournal(File directory) {
    this.directory = directory;
  }

  @Override
  public void open() throws IOException {
    File file = new File(directory, JOURNAL_FILENAME);
    if (file.isFile()) {
      ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
      try {
        boolean valid = ois.readBoolean();
        states.clear();
        if (valid) {
          Object readStates = ois.readObject();
          states.putAll((Map<? extends TransactionId, ? extends XAState>) readStates);
        }
      } catch (IOException ioe) {
        LOGGER.warn("Cannot read XA journal, truncating it", ioe);
      } catch (ClassNotFoundException cnfe) {
        LOGGER.warn("Cannot deserialize XA journal contents, truncating it", cnfe);
      } finally {
        ois.close();
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
        try {
          oos.writeObject(false);
        } finally {
          oos.close();
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(directory, JOURNAL_FILENAME)));
    try {
      oos.writeBoolean(true);
      oos.writeObject(states);
      states.clear();
    } finally {
      oos.close();
    }
  }
}
