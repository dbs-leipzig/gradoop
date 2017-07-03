
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.functions.ExtractPropertyValues;

import java.util.Set;

/**
 * Computes the number of distinct edge property values for properties
 */
public class DistinctEdgeProperties extends DistinctProperties<Edge, String> {

  @Override
  protected DataSet<Tuple2<String, Set<PropertyValue>>> extractValuePairs(LogicalGraph graph) {
    return graph.getEdges().flatMap(new ExtractPropertyValues<>());
  }
}
