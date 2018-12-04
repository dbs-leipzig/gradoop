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

package org.gradoop.dataintegration.importer.rdbmsimporter.functions;

import org.apache.flink.types.Row;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.dataintegration.importer.rdbmsimporter.metadata.RowHeader;
import org.gradoop.dataintegration.importer.rdbmsimporter.tuples.RowHeaderTuple;

import static org.gradoop.dataintegration.importer.rdbmsimporter.constants.RdbmsConstants.FK_FIELD;

/**
 * Converts database relation tuples to valid EPGM properties.
 */
public class AttributesToProperties {

  /**
   * Instance variable of class {@link AttributesToProperties}.
   */
  private static AttributesToProperties OBJ = null;

  /**
   * Singleton instance of class {@link AttributesToProperties}.
   */
  private AttributesToProperties() { }

  /**
   * Creates a single instance of class {@link AttributesToProperties}.
   *
   * @return single instance of class {@link AttributesToProperties}
   */
  public static AttributesToProperties create() {
    if (OBJ == null) {
      OBJ = new AttributesToProperties();
    }
    return OBJ;
  }

  /**
   * Converts a tuple of a database relation to EPGM properties.
   *
   * @param tuple database relation-tuple
   * @param rowheader rowheader for this relation
   * @return EPGM properties
   */
  public static Properties getProperties(Row tuple, RowHeader rowheader) {

    Logger logger = Logger.getLogger(AttributesToProperties.class);
    Properties properties = Properties.create();

    for (RowHeaderTuple rowHeaderTuple : rowheader.getRowHeader()) {
      try {
        properties.set(rowHeaderTuple.getName(),
          PropertyValueParser.parse(tuple.getField(rowHeaderTuple.getPos())));
      } catch (IndexOutOfBoundsException e) {
        logger.warn("Empty value field in column " + rowHeaderTuple.getName());
      }
    }
    return properties;
  }

  /**
   * Converts a tuple of a database relation to EPGM properties without foreign key attributes.
   *
   * @param tuple database relation-tuple
   * @param rowheader rowheader for this relation
   * @return EPGM properties
   */
  public static Properties getPropertiesWithoutFKs(Row tuple, RowHeader rowheader) {
    Properties properties = new Properties();
    for (RowHeaderTuple rowHeaderTuple : rowheader.getRowHeader()) {
      if (!rowHeaderTuple.getAttType().equals(FK_FIELD)) {
        properties.set(rowHeaderTuple.getName(),
          PropertyValueParser.parse(tuple.getField(rowHeaderTuple.getPos())));
      }
    }
    return properties;
  }
}
