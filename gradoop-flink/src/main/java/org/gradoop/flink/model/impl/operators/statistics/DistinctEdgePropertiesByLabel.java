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

package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.functions.ExtractPropertyValuesByLabel;

import java.util.Set;

/**
 * Computes the number of distinct edge property values for label - property name pairs
 */
public class DistinctEdgePropertiesByLabel
  extends DistinctProperties<Edge, Tuple2<String, String>> {

  @Override
  protected DataSet<Tuple2<Tuple2<String, String>, Set<PropertyValue>>> extractValuePairs(
    LogicalGraph graph) {
    return graph.getEdges().flatMap(new ExtractPropertyValuesByLabel<>());
  }
}
