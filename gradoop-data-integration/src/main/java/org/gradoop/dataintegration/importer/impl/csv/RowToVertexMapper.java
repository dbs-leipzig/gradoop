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
package org.gradoop.dataintegration.importer.impl.csv;

import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Map the values of a token separated row of a file to the properties of a EPGM vertex.
 * Each property value will be mapped to property name the user set.
 * This names are stored in a list.
 */
public class RowToVertexMapper implements FlatMapFunction<String, Properties> {

  /**
   * Token separator for the csv file.
   */
  private String tokenSeparator;

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
   *
   * @param tokenDelimiter in the file is used
   * @param propertyNames list of the property names
   * @param checkReoccurringHeader should the row checked for a occurring of the column names?
   */
  public RowToVertexMapper(String tokenDelimiter,
                           List<String> propertyNames, boolean checkReoccurringHeader) {
    this.tokenSeparator = tokenDelimiter;
    this.propertyNames = propertyNames;
    this.checkReoccurringHeader = checkReoccurringHeader;
  }

  @Override
  public void flatMap(String line, Collector<Properties> out) {
    Properties properties = new Properties();

    String[] propertyValues = line.split(tokenSeparator);

    boolean equals = true;
    for (int i = 0; i < propertyValues.length; i++) {
      if (!propertyValues[i].isEmpty()) {
        properties.set(propertyNames.get(i), PropertyValue.create(propertyValues[i]));
      }
      if (checkReoccurringHeader) {
        /*
         * If the line to read is equals to the header, we do not import this line
         * (because the file contains a header line, which is not a vertex).
         * In this case, we return null, else we return the line tuple.
         */
        if (!propertyValues[i].trim().equals(propertyNames.get(i).trim())) {
          equals = false;
        }
      }
    }
    if (checkReoccurringHeader && equals) {
      return;
    }
    out.collect(properties);
  }
}
