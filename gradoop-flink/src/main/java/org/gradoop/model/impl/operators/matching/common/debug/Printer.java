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

package org.gradoop.model.impl.operators.matching.common.debug;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.Logger;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyValue;

import java.util.List;
import java.util.Map;

/**
 * Base class for printing debug output using vertex and edge mappings. Both
 * map a {@link GradoopId} to a readable numeric id.
 */
@FunctionAnnotation.ForwardedFields("*")
public abstract class Printer<IN> extends RichMapFunction<IN, IN> {

  public static final String VERTEX_MAPPING = "vertexMapping";

  public static final String EDGE_MAPPING = "edgeMapping";

  protected Map<GradoopId, PropertyValue> vertexMap;

  protected Map<GradoopId, PropertyValue> edgeMap;

  protected final String prefix;

  protected final boolean isIterative;

  public Printer() {
    this(false, "");
  }

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
    getLogger().debug(String.format("%s%s",getHeader(), getDebugString(in)));
    return in;
  }

  protected abstract String getDebugString(IN in);

  protected abstract Logger getLogger();

  protected String getHeader() {
    return String.format("[%d][%s]: ",
      isIterative ? getIterationRuntimeContext().getSuperstepNumber() : 0,
      prefix != null && !prefix.isEmpty() ? prefix : " ");
  }

  private Map<GradoopId, PropertyValue> initMapping(
    List<Tuple2<GradoopId, PropertyValue>> tuples) {
    Map<GradoopId, PropertyValue> map = Maps.newHashMap();
    for (Tuple2<GradoopId, PropertyValue> tuple : tuples) {
      map.put(tuple.f0, tuple.f1);
    }
    return map;
  }

  protected List<PropertyValue> convertList(List<GradoopId> gradoopIds, boolean isVertex) {
    List<PropertyValue> result = Lists.newArrayListWithCapacity(gradoopIds.size());
    for (GradoopId gradoopId : gradoopIds) {
      result.add(isVertex ? vertexMap.get(gradoopId) : edgeMap.get(gradoopId));
    }
    return result;
  }
}
