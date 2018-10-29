/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.importer.rdbmsimporter.connection;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Connection to relational database
 */
public class RdbmsConnectionHelper {

  /**
   * Establishes a connection to a relational database via jdbc.
   *
   * @param config Configuration of relational database
   * @return Valid connection to a relational database
   */
  public static Connection getConnection(RdbmsConfig config) {

    Logger logger = Logger.getLogger(RdbmsConnectionHelper.class);
    Connection connection = null;

    try {
      RegisterDriver.register(config);
      connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPw());
    } catch (SQLException e) {
      logger.error("Cannot establish database connection : " + e);
    }

    return connection;
  }
}
