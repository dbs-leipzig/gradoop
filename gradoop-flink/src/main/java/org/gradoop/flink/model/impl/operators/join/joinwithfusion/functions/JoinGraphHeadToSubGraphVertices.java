package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.join.common.tuples.DisambiguationTupleWithVertexId;

/**
 * Creates a fused vertex from the graph head, and remembers which is the
 * original pattern's graphid
 *
 * Created by Giacomo Bergami on 16/02/17.
 */
@FunctionAnnotation.ForwardedFields("*->f1")
public class JoinGraphHeadToSubGraphVertices implements JoinFunction<GradoopId, GraphHead, DisambiguationTupleWithVertexId> {

  private final DisambiguationTupleWithVertexId tobereturned;

  public JoinGraphHeadToSubGraphVertices(GradoopId fusedGraphId) {
    tobereturned = new DisambiguationTupleWithVertexId();
    tobereturned.f0 = new Vertex();
    tobereturned.f1 = true;
    tobereturned.f0.addGraphId(fusedGraphId);
  }

  @Override
  public DisambiguationTupleWithVertexId join(GradoopId first, GraphHead second) throws
    Exception {
    tobereturned.f0.setLabel(second.getLabel());
    tobereturned.f0.setProperties(second.getProperties());
    tobereturned.f0.setId(GradoopId.get());
    tobereturned.f2 = first;
    return tobereturned;
  }
}
