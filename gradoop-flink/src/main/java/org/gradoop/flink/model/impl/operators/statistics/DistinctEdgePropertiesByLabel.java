
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
