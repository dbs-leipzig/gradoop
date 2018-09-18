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
package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.csv.metadata.MetaData;
import org.gradoop.flink.io.impl.csv.metadata.MetaDataParser;
import org.gradoop.flink.io.impl.csv.metadata.PropertyMetaData;

import java.util.stream.Collectors;

/**
 * Base class to convert an EPGM element into a CSV representation.
 *
 * @param <E> EPGM element type
 * @param <T> output tuple type
 */
public abstract class ElementToCSV<E extends Element, T extends Tuple>
  extends RichMapFunction<E , T> {
  /**
   * Constant for an empty string.
   */
  private static final String EMPTY_STRING = "";
  /**
   * Meta data that provides parsers for a specific {@link Element}.
   */
  private MetaData metaData;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.metaData = MetaDataParser.create(getRuntimeContext()
      .getBroadcastVariable(CSVDataSource.BC_METADATA));
  }

  /**
   * Returns the concatenated property values of the specified element according to the meta data.
   *
   * @param element EPGM element
   * @param type element type
   * @return property value string
   */
  String getPropertyString(E element, String type) {
    return metaData.getPropertyMetaData(type, element.getLabel()).stream()
      .map(propertyMetaData -> this.getPropertyValueString(propertyMetaData, element))
      .collect(Collectors.joining(CSVConstants.VALUE_DELIMITER));
  }

  /**
   * Returns the string representation of the property value matching the given property metadata.
   * If no matching property is found, an empty string is returned.
   *
   * @param propertyMetaData property metadata of the wanted property value
   * @param element EPGM element containing the property value
   * @return string representation of matched property value or empty string
   */
  private String getPropertyValueString(PropertyMetaData propertyMetaData, E element) {
    PropertyValue p = element.getPropertyValue(propertyMetaData.getKey());
    // Only properties with matching type get returned
    // to prevent writing values with the wrong type metadata
    if (p != null && MetaDataParser.getTypeString(p).equals(propertyMetaData.getTypeString())) {
      return propertyValueToCsvString(p);
    }
    return EMPTY_STRING;
  }

  /**
   * Returns an escaped string representation suitable for CSV.
   *
   * @param p property value
   * @return escaped CSV string
   */
  private String propertyValueToCsvString(PropertyValue p) {
    if (p.isList()) {
      return String.format("[%s]", p.getList().stream()
        .map(this::primitivePropertyValueToCSVString)
        .collect(Collectors.joining(CSVConstants.LIST_DELIMITER)));
    } else if (p.isSet()) {
      return String.format("[%s]", p.getSet().stream()
        .map(this::primitivePropertyValueToCSVString)
        .collect(Collectors.joining(CSVConstants.LIST_DELIMITER)));
    } else if (p.isMap()) {
      return String.format("{%s}", p.getMap().entrySet().stream()
        .map(e -> primitivePropertyValueToCSVString(e.getKey()) +
          CSVConstants.MAP_SEPARATOR +
          primitivePropertyValueToCSVString(e.getValue()))
        .collect(Collectors.joining(CSVConstants.LIST_DELIMITER)));
    } else {
      return primitivePropertyValueToCSVString(p);
    }
  }

  /**
   * Returns an escaped string representation of a primitive property value for CSV.
   *
   * @param p property value
   * @return escaped CSV string
   */
  private String primitivePropertyValueToCSVString(PropertyValue p) {
    if (p.getType() == String.class) {
      return StringEscaper.escape(p.getString(), CSVConstants.ESCAPED_CHARACTERS);
    }
    return p.toString();
  }
}
