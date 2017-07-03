
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
