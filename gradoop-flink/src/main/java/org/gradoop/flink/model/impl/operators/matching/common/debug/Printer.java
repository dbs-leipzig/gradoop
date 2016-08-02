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

package org.gradoop.flink.model.impl.operators.matching.common.debug;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.List;
import java.util.Map;

/**
 * Base class for printing debug output using vertex and edge mappings. Both
 * map a {@link GradoopId} to a readable numeric id.
 *
 * @param <IN> input type
 */
@FunctionAnnotation.ForwardedFields("*")
public abstract class Printer<IN> extends RichMapFunction<IN, IN> {

  /**
   * Broadcast set name for vertex mapping
   */
  public static final String VERTEX_MAPPING = "vertexMapping";

  /**
   * Broadcast set name for edge mapping
   */
  public static final String EDGE_MAPPING = "edgeMapping";

  /**
   * Mapping gradoopId -> propertyValue
   *
   * The value is used to represent the vertex with the corresponding id.
   */
  protected Map<GradoopId, PropertyValue> vertexMap;

  /**
   * Mapping gradoopId -> propertyValue
   *
   * The value is used to represent the edge with the corresponding id.
   */
  protected Map<GradoopId, PropertyValue> edgeMap;

  /**
   * String is put in front of debug output.
   */
  protected final String prefix;

  /**
   * Used to differ between iterative and non-iterative runtime context.
   */
  protected final boolean isIterative;

  /**
   * Constructor
   */
  public Printer() {
    this(false, "");
  }

  /**
   * Constructor
   *
   * @param isIterative true, if called in iterative context
   * @param prefix prefix for output
   */
  public Printer(boolean isIterative, String prefix) {
    this.isIterative  = isIterative;
    this.prefix       = prefix;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    List<Tuple2<GradoopId, PropertyValue>> vertexMapping = getRuntimeContext()
      .getBroadcastVariable(VERTEX_MAPPING);
    vertexMap = initMapping(vertexMapping);
    List<Tuple2<GradoopId, PropertyValue>> edgeMapping = getRuntimeContext()
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
  protected String getHeader() {
    return String.format("[%d][%s]: ",
      isIterative ? getIterationRuntimeContext().getSuperstepNumber() : 0,
      prefix != null && !prefix.isEmpty() ? prefix : " ");
  }

  /**
   * Used to initialize vertex and edge mapping.
   *
   * @param tuples broadcast set tuples
   * @return mapping
   */
  private Map<GradoopId, PropertyValue> initMapping(
    List<Tuple2<GradoopId, PropertyValue>> tuples) {
    Map<GradoopId, PropertyValue> map = Maps.newHashMap();
    for (Tuple2<GradoopId, PropertyValue> tuple : tuples) {
      map.put(tuple.f0, tuple.f1);
    }
    return map;
  }

  /**
   * Converts a list of GradoopIds into a list of corresponding property
   * values.
   *
   * @param ids       gradoop ids
   * @param isVertex  true - use vertex mapping, false - use edge mapping
   * @return list of property values
   */
  protected List<PropertyValue> convertList(List<GradoopId> ids,
    boolean isVertex) {
    List<PropertyValue> result = Lists.newArrayListWithCapacity(ids.size());
    for (GradoopId gradoopId : ids) {
      result.add(isVertex ? vertexMap.get(gradoopId) : edgeMap.get(gradoopId));
    }
    return result;
  }
}
