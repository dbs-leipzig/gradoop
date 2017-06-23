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
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.csv.metadata.MetaData;
import org.gradoop.flink.io.impl.csv.metadata.MetaDataParser;

import java.util.stream.Collectors;

/**
 * Base class to convert an EPGM element into a CSV representation.
 *
 * @param <E> EPGM element type
 * @param <T> output tuple type
 */
public abstract class ElementToCSV<E extends Element, T extends Tuple> extends RichMapFunction<E , T> {
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
   * @return property value string
   */
  String getPropertyString(E element) {
    return metaData.getPropertyMetaData(element.getLabel()).stream()
      .map(metaData -> element.getPropertyValue(metaData.getKey()))
      .map(value -> value == null ? EMPTY_STRING : value.toString())
      .collect(Collectors.joining(CSVConstants.VALUE_DELIMITER));
  }
}
