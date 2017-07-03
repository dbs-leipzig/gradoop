
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.TripleWithDirection;

/**
 * Clones and reverses the incoming triple:
 *
 * (e,s,t,c) -> (e,s,t,true,c),(e,t,s,false,c)
 *
 * Forwarded fields:
 *
 * f0->f0: edge id
 * f3->f4: query candidates
 */
@FunctionAnnotation.ForwardedFields("f0;f3->f4")
public class CloneAndReverse implements
  FlatMapFunction<TripleWithCandidates<GradoopId>, TripleWithDirection> {

  /**
   * Reduce instantiations
   */
  private final TripleWithDirection reuseTuple;

  /**
   * Constructor
   */
  public CloneAndReverse() {
    reuseTuple = new TripleWithDirection();
  }

  @Override
  public void flatMap(TripleWithCandidates<GradoopId> tripleWithCandidates,
    Collector<TripleWithDirection> collector) throws Exception {
    reuseTuple.setEdgeId(tripleWithCandidates.getEdgeId());
    reuseTuple.setCandidates(tripleWithCandidates.getCandidates());

    reuseTuple.setSourceId(tripleWithCandidates.getSourceId());
    reuseTuple.setTargetId(tripleWithCandidates.getTargetId());
    reuseTuple.setOutgoing(true);
    collector.collect(reuseTuple);

    reuseTuple.setSourceId(tripleWithCandidates.getTargetId());
    reuseTuple.setTargetId(tripleWithCandidates.getSourceId());
    reuseTuple.setOutgoing(false);
    collector.collect(reuseTuple);
  }
}
