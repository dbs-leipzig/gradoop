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

import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.RdbmsType;

/**
 * Manages assigning of relational database management type
 */
public class RdbmsTypeChooser {

  /**
   * Instance variable of class {@link RdbmsTypeChooser}.
   */
  private static RdbmsTypeChooser OBJ = null;

  /**
   * Singelton class to choose and assign a database management system type.
   */
  private RdbmsTypeChooser() { }

  /**
   * Creates a single instance of {@link RdbmsTypeChooser}.
   *
   * @return single instance of {@link RdbmsTypeChooser}
   */
  public static RdbmsTypeChooser create() {
    if (OBJ == null) {
      OBJ = new RdbmsTypeChooser();
    }
    return OBJ;
  }

  /**
   * Assigns a database management system type.
   *
   * @param rdbmsName name of database instance
   * @return type of database management system
   */
  public static RdbmsType choose(String rdbmsName) throws UnsupportedTypeException {
    RdbmsType rdbmsType;

    switch (rdbmsName) {
    case "derby":
    case "microsoft sql server":
    case "oracle":
      rdbmsType = RdbmsType.SQLSERVER_TYPE;
      break;

    case "posrgresql":
    case "mysql":
    case "h2":
    case "sqlite":
    case "hsqldb":
    default:
      rdbmsType = RdbmsType.MYSQL_TYPE;
      break;
    }
    return rdbmsType;
  }
}
