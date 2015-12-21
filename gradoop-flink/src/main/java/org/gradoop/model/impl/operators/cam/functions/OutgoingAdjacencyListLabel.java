package org.gradoop.model.impl.operators.cam.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.cam.tuples.EdgeLabel;
import org.gradoop.model.impl.operators.cam.tuples.VertexLabel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OutgoingAdjacencyListLabel implements
  GroupReduceFunction<EdgeLabel, VertexLabel> {

  @Override
  public void reduce(Iterable<EdgeLabel> outgoingEdgeLabels,
    Collector<VertexLabel> collector) throws Exception {

    Boolean first = true;
    GradoopId vertexId = null;
    GradoopId graphId = null;

    List<String> adjacencyListEntries = new ArrayList<>();

    for(EdgeLabel edgeLabel : outgoingEdgeLabels) {
      if(first) {
        graphId = edgeLabel.getGraphId();
        vertexId = edgeLabel.getSourceId();
        first = false;
      }

      adjacencyListEntries.add("\n  -" + edgeLabel.getEdgeLabel() + "->"
        + edgeLabel.getTargetLabel());
    }

    Collections.sort(adjacencyListEntries);

    collector.collect(new VertexLabel(
      graphId,
      vertexId,
      StringUtils.join(adjacencyListEntries,"")
    ));
  }
}
