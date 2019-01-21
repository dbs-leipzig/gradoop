/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

package org.gradoop.dataintegration.importer.impl.rdbms.constants;

/**
 * Stores constants for relational database to graph conversion.
 */
public class RdbmsConstants {

  /**
   * Vertex key identifier for primary keys.
   */
  public static final String PK_ID = "*#primary_key_identifier#*";

  /**
   * Field identifier for primary keys.
   */
  public static final String PK_FIELD = "pk";

  /**
   * Field identifier for foreign keys.
   */
  public static final String FK_FIELD = "fk";

  /**
   * Field identifier for further attributes.
   */
  public static final String ATTRIBUTE_FIELD = "att";

  /**
   * Broadcast variable constant.
   */
  public static final String BROADCAST_VARIABLE = "broadcastVariable";

  /**
   * Database management system identifier.
   */
  public enum RdbmsType {
    /**
     * Identifier for mysql,mariadb,postgresql,h2,hsqldb management systems.
     */
    MYSQL_TYPE,
    /**
     * Identifier for sql-server, oracle,derby,sql:2018 standard management systems.
     */
    SQLSERVER_TYPE
  }
}
