package org.gradoop.model.impl.operators.cam.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.cam.tuples.GraphHeadLabel;
import org.gradoop.model.impl.operators.cam.tuples.VertexLabel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AdjacencyMatrixLabel implements
  GroupReduceFunction<VertexLabel, GraphHeadLabel> {

  @Override
  public void reduce(Iterable<VertexLabel> vertexLabels,
    Collector<GraphHeadLabel> collector) throws Exception {


    Boolean first = true;
    GradoopId graphId = null;

    List<String> matrixRows = new ArrayList<>();

    for(VertexLabel vertexLabel : vertexLabels) {
      if(first) {
        graphId = vertexLabel.getGraphId();
        first = false;
      }

      matrixRows.add("\n " + vertexLabel.getLabel());
    }

    Collections.sort(matrixRows);

    collector.collect(new GraphHeadLabel(
      graphId,
      StringUtils.join(matrixRows,"")
    ));


  }
}
