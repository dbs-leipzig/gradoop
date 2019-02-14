/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.dataintegration.transformation.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * This {@link CoGroupFunction} accumulates all properties that might be send to a vertex and
 * stores them in a {@link PropertyValue} list.
 *
 * @param <V> The vertex type.
 */
public class AccumulatePropagatedValues<V extends EPGMVertex>
  implements CoGroupFunction<Tuple2<GradoopId, PropertyValue>, V, V> {

  /**
   * The property key where the PropertyValue list should be stored at the target vertices.
   */
  private final String targetVertexPropertyKey;

  /**
   * Labels of vertices where the propagated property should be set.
   */
  private final Set<String> targetVertexLabels;

  /**
   * The constructor of the co group function for accumulation of collected property values.
   *
   * @param targetVertexPropertyKey The property key where the PropertyValue list should be
   *                                stored at the target vertices.
   * @param targetVertexLabels      The set of labels of elements where the property should be
   *                                set. (Use {@code null} for all vertices.)
   */
  public AccumulatePropagatedValues(String targetVertexPropertyKey,
    Set<String> targetVertexLabels) {
    this.targetVertexPropertyKey = Objects.requireNonNull(targetVertexPropertyKey);
    this.targetVertexLabels = targetVertexLabels;
  }

  @Override
  public void coGroup(Iterable<Tuple2<GradoopId, PropertyValue>> propertyValues,
                      Iterable<V> elements, Collector<V> out) {
    // should only contain one vertex, based on the uniqueness of gradoop ids
    Iterator<V> iterator = elements.iterator();
    if (!iterator.hasNext()) {
      return;
    }
    V targetVertex = iterator.next();
    // If the vertex is not whitelisted by the targetVertexLabels list,
    // forward it without modification.
    if (targetVertexLabels != null && !targetVertexLabels.contains(targetVertex.getLabel())) {
      out.collect(targetVertex);
      return;
    }

    // collect values of neighbors
    List<PropertyValue> values = new ArrayList<>();
    propertyValues.forEach(t -> values.add(t.f1));

    // Add to vertex if and only if at least one property was propagated.
    if (!values.isEmpty()) {
      targetVertex.setProperty(targetVertexPropertyKey, PropertyValue.create(values));
    }

    out.collect(targetVertex);
  }
}
