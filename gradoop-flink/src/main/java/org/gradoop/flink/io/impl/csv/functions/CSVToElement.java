/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.csv.metadata.MetaData;
import org.gradoop.flink.io.impl.csv.metadata.MetaDataParser;
import org.gradoop.flink.io.impl.csv.metadata.PropertyMetaData;

import java.util.List;

/**
 * Base class for reading an {@link Element} from CSV. Handles the {@link MetaData} which is
 * required to parse the property values.
 *
 * @param <T> input tuple type
 * @param <E> EPGM element type
 */
abstract class CSVToElement<T extends Tuple, E extends Element>
  extends RichMapFunction<T, E> {
  /**
   * Used to separate the tokens (id, label, values) in the CSV file.
   */
  public static final String TOKEN_SEPARATOR = ";";
  /**
   * Used to separate the property values in the CSV file.
   */
  static final String VALUE_DELIMITER = "\\|";
  /**
   * Stores the properties for the {@link Element} to be parsed.
   */
  private final Properties properties;
  /**
   * Meta data that provides parsers for a specific {@link Element}.
   */
  private MetaData metaData;

  /**
   * Constructor
   */
  CSVToElement() {
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
   * @param label element label
   * @param propertyValues string representation of elements property values
   * @return parsed properties
   */
  Properties parseProperties(String label, String[] propertyValues) {
    List<PropertyMetaData> metaDataList = metaData.getPropertyMetaData(label);
    properties.clear();
    for (int i = 0; i < propertyValues.length; i++) {
      if (propertyValues[i].length() > 0) {
        properties.set(metaDataList.get(i).getKey(),
          metaDataList.get(i).getValueParser().apply(propertyValues[i]));
      }
    }
    return properties;
  }
}
