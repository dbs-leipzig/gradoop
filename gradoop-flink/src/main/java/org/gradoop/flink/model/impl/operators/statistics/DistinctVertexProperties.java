
package org.gradoop.flink.model.impl.operators.statistics;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.functions.ExtractPropertyValues;

import java.util.Set;

/**
 * Computes the number of distinct property values for vertex label - property name pairs
 */
public class DistinctVertexProperties extends DistinctProperties<Vertex, String> {

  @Override
  protected DataSet<Tuple2<String, Set<PropertyValue>>> extractValuePairs(LogicalGraph graph) {
    return graph.getVertices().flatMap(new ExtractPropertyValues<>());
  }
}
