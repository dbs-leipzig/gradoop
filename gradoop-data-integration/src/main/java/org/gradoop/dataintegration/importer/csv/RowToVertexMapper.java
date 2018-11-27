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
package org.gradoop.dataintegration.importer.csv;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Map the values of a token separated row of a file to the properties of a EPGM vertex.
 * Each property value will be mapped to property name the user set.
 * This names are stored in a list.
 */
public class RowToVertexMapper implements MapFunction<String, Properties> {

  /**
   * Token separator for the csv file.
   */
  private String tokenSperator;

  /**
   * The path to the csv file
   */
  private String filePath;

  /**
   * The name of the properties
   */
  private List<String> propertyNames;

  /**
   * True, if the user want to check if each row of the file is equals to the header row.
   */
  private boolean checkReoccurringHeader;

  /**
   * Create a new RowToVertexMapper
   * @param filePath the csv file is stored
   * @param tokenDelimiter in the file is used
   * @param propertyNames list of the property names
   * @param checkReoccurringHeader should the row checked for a occurring of the column names?
   */
  public RowToVertexMapper(String filePath, String tokenDelimiter,
          List<String> propertyNames, boolean checkReoccurringHeader) {
    this.filePath = filePath;
    this.tokenSperator = tokenDelimiter;
    this.propertyNames = propertyNames;
    this.checkReoccurringHeader = checkReoccurringHeader;
  }

  @Override
  public Properties map(final String line) {
    return parseProperties(line, propertyNames);
  }

  /**
   * Map each label to the occurring properties.
   * @param line one row of the csv file, contains all the property values of one vertex
   * @param propertyNames identifier of the property values
   * @return the properties of the vertex
   */
  private Properties parseProperties(String line, List<String> propertyNames) {

    Properties properties = new Properties();

    String[] propertyValues = line.split(tokenSperator);

    if (checkReoccurringHeader) {
      /*
       * If the line to read is equals to the header, we do not import this line
       * (because the file contains a header line, which is not a vertex).
       * In this case, we return null, else we return the line tuple.
       */
      boolean equals = false;
      if (propertyValues.length == propertyNames.size()) {
        for (int i = 0; i < propertyValues.length; i++) {
          if (propertyValues[i].trim().equals(propertyNames.get(i).trim())) {
            equals = true;
          } else {
            equals = false;
            break;
          }
        }
        if (equals) {
          return null;
        }
      }
    }

    for (int i = 0; i < propertyValues.length; i++) {
      if (propertyValues[i].length() > 0) {
        properties.set(propertyNames.get(i),
                  PropertyValue.create(propertyValues[i]));
      }
    }
    return properties;
  }
}
