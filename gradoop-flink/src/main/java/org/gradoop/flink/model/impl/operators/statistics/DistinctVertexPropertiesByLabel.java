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
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.functions.ExtractPropertyValuesByLabel;

import java.util.Set;

/**
 * Computes the number of distinct vertex property values for label - property name pairs
 */
public class DistinctVertexPropertiesByLabel
  extends DistinctProperties<Vertex, Tuple2<String, String>> {

  @Override
  protected DataSet<Tuple2<Tuple2<String, String>, Set<PropertyValue>>> extractValuePairs(
    LogicalGraph graph) {
    return graph.getVertices().flatMap(new ExtractPropertyValuesByLabel<>());
  }
}
