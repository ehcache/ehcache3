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
package org.ehcache.demos.peeper;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Ludovic Orban
 */
public class DataStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataStore.class);

  public final DataCache dataCache = new DataCache();

  private Connection connection;


  @SuppressFBWarnings("DMI_EMPTY_DB_PASSWORD")
  public void init() throws Exception {
    dataCache.setupCache();
    Class.forName("org.h2.Driver");
    connection = DriverManager.getConnection("jdbc:h2:~/ehcache-demo-peeper", "sa", "");

    try (Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE IF NOT EXISTS PEEPS (" +
                        "id bigint auto_increment primary key," +
                        "PEEP_TEXT VARCHAR(142) NOT NULL" +
                        ")");
      connection.commit();
    }
  }


  public synchronized void addPeep(String peepText) throws Exception {
    LOGGER.info("Adding peep into DB");
    try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO PEEPS (PEEP_TEXT) VALUES (?)")) {
      preparedStatement.setString(1, peepText);
      preparedStatement.execute();
      connection.commit();

      //For any add in database; clear the cache
      //LOGGER.info(" Added new peep in DB, clearing cache...");
      LOGGER.info("Clearing peeps cache");
      dataCache.clearCache();
    }
  }

  public synchronized List<String> findAllPeeps() throws Exception {
    List<String> result = new ArrayList<>();
    //find from cache 1st
    List<String> fromCache = dataCache.getFromCache();
    if (fromCache != null) {
      LOGGER.info("Getting peeps from cache");
      return fromCache;
    }

    LOGGER.info("Loading peeps from DB");
    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM PEEPS")) {

        while (resultSet.next()) {
          String peepText = resultSet.getString("PEEP_TEXT");
          result.add(peepText);
        }
      }
    }

    LOGGER.info("Filling cache with peeps");
    dataCache.addToCache(result);
    return result;
  }

  public void close() throws Exception {
    dataCache.close();
    connection.close();
  }

}
