package org.gradoop.flink.algorithms.fsm.canonicalization;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.predictable
  .PredictableTransactionsGenerator;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.junit.Test;


public class CAMLabelerTest extends GradoopFlinkTestBase{
//  @Test
//  public void label() throws Exception {
//
//    PredictableTransactionsGenerator generator =
//      new PredictableTransactionsGenerator(1, 1, true, getConfig());
//
//    DataSet<GraphTransaction> transactions = generator
//      .execute()
//      .getTransactions()
//      .flatMap(new DistinctConnectedSubgraphs());
//
//    LogicalGraph graph = GraphCollection
//      .fromTransactions(transactions)
//      .reduce(new ReduceCombination())
//      .subgraph(new FilterFunction<Vertex>() {
//        @Override
//        public boolean filter(Vertex value) throws Exception {
//          return value.getLabel().equals("A");
//        }
//      }, new True<Edge>())
//      .callForCollection();
//
//    System.out.println(graph.getVertices().collect());
//
//  }

}