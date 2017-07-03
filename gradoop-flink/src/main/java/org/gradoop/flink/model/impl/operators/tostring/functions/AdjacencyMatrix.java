package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * creates a string representation of an adjacency matrix
 */
public class AdjacencyMatrix implements
  GroupReduceFunction<VertexString, GraphHeadString> {

  @Override
  public void reduce(Iterable<VertexString> vertexLabels,
    Collector<GraphHeadString> collector) throws Exception {

    Boolean first = true;
    GradoopId graphId = null;

    List<String> matrixRows = new ArrayList<>();

    for (VertexString vertexString : vertexLabels) {
      if (first) {
        graphId = vertexString.getGraphId();
        first = false;
      }

      matrixRows.add("\n " + vertexString.getLabel());
    }

    Collections.sort(matrixRows);

    collector.collect(
      new GraphHeadString(graphId, StringUtils.join(matrixRows, "")));
  }
}
