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
package org.gradoop.flink.model.impl.operators.matching.common.debug;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.List;
import java.util.Map;

/**
 * Base class for printing debug output using vertex and edge mappings. Both
 * map an internal id to a readable numeric id.
 *
 * @param <IN> input type
 * @param <K> key type
 */
@FunctionAnnotation.ForwardedFields("*")
public abstract class Printer<IN, K> extends RichMapFunction<IN, IN> {
  /**
   * Broadcast set name for vertex mapping
   */
  public static final String VERTEX_MAPPING = "vertexMapping";
  /**
   * Broadcast set name for edge mapping
   */
  public static final String EDGE_MAPPING = "edgeMapping";
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(Printer.class);
  /**
   * Mapping gradoopId -> propertyValue
   *
   * The value is used to represent the vertex with the corresponding id.
   */
  protected Map<K, PropertyValue> vertexMap;
  /**
   * Mapping gradoopId -> propertyValue
   *
   * The value is used to represent the edge with the corresponding id.
   */
  protected Map<K, PropertyValue> edgeMap;
  /**
   * String is put in front of debug output.
   */
  protected final String prefix;
  /**
   * Used to differ between iterative and non-iterative runtime context.
   */
  protected final boolean isIterative;
  /**
   * Number to display if not in iterative context but iteration is set.
   */
  private final int iterationNumber;
  /**
   * Constructor
   */
  public Printer() {
    this(false, "");
  }

  /**
   * Constructor
   *
   * Automatically determines the current iteration when instantiated in Flink iteration.
   *
   * @param isIterative true, if called in a Flink iterative context (i.e. bulk/delta iteration)
   * @param prefix      prefix for output
   */
  public Printer(boolean isIterative, String prefix) {
    this.isIterative     = isIterative;
    this.iterationNumber = 0;
    this.prefix          = prefix;
  }

  /**
   * Constructor
   *
   * Requires the manual definition of the current iteration. This is necessary when called
   * outside a Flink iteration, but in an iterative context (e.g. a Java loop).
   *
   * @param iterationNumber current iteration
   * @param prefix          prefix for debug string
   */
  public Printer(int iterationNumber, String prefix) {
    this.isIterative     = false;
    this.iterationNumber = iterationNumber;
    this.prefix          = prefix;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    List<Tuple2<K, PropertyValue>> vertexMapping = getRuntimeContext()
      .getBroadcastVariable(VERTEX_MAPPING);
    vertexMap = initMapping(vertexMapping);
    List<Tuple2<K, PropertyValue>> edgeMapping = getRuntimeContext()
      .getBroadcastVariable(EDGE_MAPPING);
    edgeMap = initMapping(edgeMapping);
  }

  @Override
  public IN map(IN in) throws Exception {
    getLogger().debug(String.format("%s%s", getHeader(), getDebugString(in)));
    return in;
  }

  /**
   * Returns the debug string representation of the concrete Object.
   *
   * @param in input object
   * @return debug string representation for input object
   */
  protected abstract String getDebugString(IN in);

  /**
   * Returns the logger for the concrete subclass.
   *
   * @return logger
   */
  protected abstract Logger getLogger();

  /**
   * Builds the header for debug string which contains the prefix and the
   * superstep number (0 if non-iterative).
   *
   * @return debug header
   */
  private String getHeader() {
    return String.format("[%d][%s]: ",
      isIterative ? getIterationRuntimeContext().getSuperstepNumber() : iterationNumber,
      prefix != null && !prefix.isEmpty() ? prefix : " ");
  }

  /**
   * Used to initialize vertex and edge mapping.
   *
   * @param tuples broadcast set tuples
   * @return mapping
   */
  private Map<K, PropertyValue> initMapping(
    List<Tuple2<K, PropertyValue>> tuples) {
    Map<K, PropertyValue> map = Maps.newHashMap();
    for (Tuple2<K, PropertyValue> tuple : tuples) {
      map.put(tuple.f0, tuple.f1);
    }
    return map;
  }

  /**
   * Converts a list of ids into a list of corresponding property values.
   *
   * @param ids       gradoop ids
   * @param isVertex  true - use vertex mapping, false - use edge mapping
   * @return list of property values
   */
  protected List<PropertyValue> convertList(List<K> ids, boolean isVertex) {
    List<PropertyValue> result = Lists.newArrayListWithCapacity(ids.size());
    for (K gradoopId : ids) {
      result.add(isVertex ? vertexMap.get(gradoopId) : edgeMap.get(gradoopId));
    }
    return result;
  }

  /**
   * Checks if log level is debug and the mappings are initialized.
   *
   * @param vertexMapping mapping between vertex id and debug property value
   * @param edgeMapping   mapping between edge id and debug property value
   * @param <K>           key type
   * @return true, iff logging is enabled
   */
  private static <K> boolean isDebugEnabled(
    DataSet<Tuple2<K, PropertyValue>> vertexMapping,
    DataSet<Tuple2<K, PropertyValue>> edgeMapping) {
    return LOG.isDebugEnabled() && vertexMapping != null && edgeMapping != null;
  }

  /**
   * Prints out logging information if logging is enabled and mapping information is available.
   *
   * @param dataSet       dataset to be logged
   * @param printer       prints each dataset row (or a subset of it)
   * @param vertexMapping mapping between vertex id and debug property value
   * @param edgeMapping   mapping between edge id and debug property value
   * @param <T>           dataset type
   * @param <K>           key type
   * @return unmodified dataset
   */
  public static <T, K> DataSet<T> log(DataSet<T> dataSet, Printer<T, K> printer,
    DataSet<Tuple2<K, PropertyValue>> vertexMapping,
    DataSet<Tuple2<K, PropertyValue>> edgeMapping) {
    if (isDebugEnabled(vertexMapping, edgeMapping)) {
      return dataSet
        .map(printer)
        .withBroadcastSet(vertexMapping, Printer.VERTEX_MAPPING)
        .withBroadcastSet(edgeMapping, Printer.EDGE_MAPPING);
    } else {
      return dataSet;
    }
  }
}
