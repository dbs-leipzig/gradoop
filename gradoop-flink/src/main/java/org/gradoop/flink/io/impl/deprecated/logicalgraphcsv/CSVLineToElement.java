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
package org.gradoop.flink.io.impl.deprecated.logicalgraphcsv;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.csv.metadata.PropertyMetaData;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Base class for reading an {@link Element} from CSV. Handles the {@link MetaData} which is
 * required to parse the property values.
 *
 * @param <E> EPGM element type
 */
abstract class CSVLineToElement<E extends Element> extends RichMapFunction<String, E> {
  /**
   * Stores the properties for the {@link Element} to be parsed.
   */
  private final Properties properties;
  /**
   * Needed for splitting the input.
   */
  private final String valueDelimiter = Pattern.quote(CSVConstants.VALUE_DELIMITER);
  /**
   * Meta data that provides parsers for a specific {@link Element}.
   */
  private MetaData metaData;

  /**
   * Constructor
   */
  CSVLineToElement() {
    this.properties = Properties.create();
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.metaData = MetaDataParser.create(getRuntimeContext()
      .getBroadcastVariable(CSVDataSource.BC_METADATA));
  }

  /**
   * Parses the given property values according to the meta data associated with the specified
   * label.
   *
   * @param type element type
   * @param label element label
   * @param propertyValueString string representation of elements' property values
   * @return parsed properties
   */
  Properties parseProperties(String type, String label, String propertyValueString) {
    String[] propertyValues = propertyValueString.split(valueDelimiter);
    List<PropertyMetaData> metaDataList = metaData.getPropertyMetaData(type, label);
    properties.clear();
    for (int i = 0; i < propertyValues.length; i++) {
      if (propertyValues[i].length() > 0) {
        properties.set(metaDataList.get(i).getKey(),
          metaDataList.get(i).getValueParser().apply(propertyValues[i]));
      }
    }
    return properties;
  }

  /**
   * Splits the specified string.
   *
   * Note: Using {@link Pattern#split(CharSequence)} leads to a significant performance loss.
   *
   * @param s string
   * @param limit resulting array length
   * @return tokens
   */
  protected String[] split(String s, int limit) {
    return s.split(CSVConstants.TOKEN_DELIMITER, limit);
  }
}
