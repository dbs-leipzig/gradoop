package org.gradoop.flink.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.algorithms.fsm.dimspan.DIMSpan;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.functions.conversion.ToMinLabeledGraphStringString;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.representation.transactional.GraphTransaction;

/**
 * Gradoop operator wrapping the DIMSpan algorithm for transactional frequent subgraph mining.
 */
public class TransactionalFSM implements UnaryCollectionToCollectionOperator {

  /**
   * Flink-based implementation of the DIMSpan algorithm
   */
  private final DIMSpan dimSpan;

  /**
   * Constructor.
   *
   * @param minSupport minimum support threshold.
   */
  public TransactionalFSM(float minSupport) {
    // only directed mode for use withing Gradoop programs as EPGM is directed
    DIMSpanConfig fsmConfig = new DIMSpanConfig(minSupport, true);
    dimSpan = new DIMSpan(fsmConfig);
  }

  /**
   * Unit testing constructor.
   *
   * @param fsmConfig externally configured DIMSpan configuration
   */
  public TransactionalFSM(DIMSpanConfig fsmConfig) {
    dimSpan = new DIMSpan(fsmConfig);
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {

    // convert Gradoop graph collection to DIMSpan input format
    DataSet<LabeledGraphStringString> input = collection
      .toTransactions()
      .getTransactions()
      .map(new ToMinLabeledGraphStringString());

    // run DIMSpan
    DataSet<GraphTransaction> output = dimSpan.execute(input);

    // convert to Gradoop graph collection
    return GraphCollection
      .fromTransactions(new GraphTransactions(output, collection.getConfig()));
  }

  @Override
  public String getName() {
    return DIMSpan.class.getSimpleName();
  }
}
