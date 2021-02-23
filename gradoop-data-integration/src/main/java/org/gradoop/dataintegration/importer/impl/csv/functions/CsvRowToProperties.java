/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.importer.impl.csv.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Map the values of a token separated row of a file to the properties of a EPGM element.
 * Each property value will be mapped to the property name the user set via parameter.
 *
 * @param <T> input type
 */
public class CsvRowToProperties<T extends Tuple> implements FlatMapFunction<T, Properties> {

  /**
   * The name of the properties
   */
  private List<String> propertyNames;

  /**
   * True, if the user want to check if each row of the file is equals to the header row
   */
  private boolean checkReoccurringHeader;

  /**
   * Reduce object instantiations
   */
  private Properties reuseProperties;

  /**
   * Create a new RowToVertexMapper
   *
   * @param propertyNames list of the property names
   * @param checkReoccurringHeader should the row checked for a occurring of the column names?
   */
  public CsvRowToProperties(List<String> propertyNames, boolean checkReoccurringHeader) {
    this.propertyNames = Objects.requireNonNull(propertyNames);
    this.checkReoccurringHeader = checkReoccurringHeader;
    this.reuseProperties = new Properties();
  }

  @Override
  public void flatMap(T fields, Collector<Properties> out) {
    String[] propertyValues = new String[fields.getArity()];
    for (int i = 0; i < fields.getArity(); i++) {
      propertyValues[i] = fields.getField(i);
    }

    // If the line to read is equals to the header and the checkReoccurringHeader flag is set to
    // TRUE, we do not import this line.
    if (checkReoccurringHeader && propertyNames.containsAll(Arrays.asList(propertyValues))) {
      return;
    }

    // clear the properties
    reuseProperties.clear();

    for (int i = 0; i < propertyValues.length; i++) {
      // if a value is empty, do not add a property
      if (!propertyValues[i].isEmpty()) {
        reuseProperties.set(propertyNames.get(i), PropertyValue.create(propertyValues[i]));
      }
    }
    out.collect(reuseProperties);
  }
}
