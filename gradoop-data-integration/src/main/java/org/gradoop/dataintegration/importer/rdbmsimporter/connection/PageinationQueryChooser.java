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

import org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.RdbmsType;

/**
 * Chooses a fitting pageination prepared statement depending on connected
 * managementsystem
 *
 */
public class PageinationQueryChooser {

  /**
   * Chooses a fitting pageination prepared statement depending on connected
   * management system
   *
   * @param rdbmsType Database identifier of connected database
   * @return Valid sql pageination prepared statement
   */
  public static String choose(RdbmsType rdbmsType) {
    String pageinationQuery = "";

    switch (rdbmsType) {
    case MYSQL_TYPE:
    default:
      pageinationQuery = " LIMIT ? OFFSET ?";
      break;
    case SQLSERVER_TYPE:
      pageinationQuery = " ORDER BY (1) OFFSET (?) ROWS FETCH NEXT (?) ROWS ONLY";
      break;
    }
    return pageinationQuery;
  }
}
