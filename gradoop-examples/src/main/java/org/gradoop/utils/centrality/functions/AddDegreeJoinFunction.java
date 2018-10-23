package org.gradoop.utils.centrality.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.tuples.WithCount;

public class AddDegreeJoinFunction implements JoinFunction<Vertex, WithCount<GradoopId>, Vertex> {

  private final String degreeKey;

  public AddDegreeJoinFunction(String degreeKey) {
    this.degreeKey = degreeKey;
  }

  @Override
  public Vertex join(Vertex vertex, WithCount<GradoopId> gradoopIdWithCount) throws Exception {

    vertex.setProperty(degreeKey, gradoopIdWithCount.f1);

    return vertex;
  }
}
