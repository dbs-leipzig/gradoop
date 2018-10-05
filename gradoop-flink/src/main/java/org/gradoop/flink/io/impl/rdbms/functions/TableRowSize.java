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

package org.gradoop.flink.io.impl.rdbms.functions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Provides the number of rows of a relational database
 */
public class TableRowSize {

  /**
   * Queries a relational database to get the number of rows
   *
   * @param con Valid jdbc database connection
   * @param tableName Name of database table
   * @return Number of rows of database
   * @throws SQLException
   */
  public static int getTableRowCount(Connection con, String tableName) throws SQLException {
    int rowCount = 0;
    Statement st = con.createStatement();
    try {
      ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + tableName);
      if (rs.next()) {
        rowCount = rs.getInt(1);
        rs.close();
      } else {
        rowCount = 0;
      }
    } finally {
      st.close();
    }
    return rowCount;
  }
}
