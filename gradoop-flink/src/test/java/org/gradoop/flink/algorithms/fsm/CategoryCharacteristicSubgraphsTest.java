package org.gradoop.flink.algorithms.fsm;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.ccs.CategoryCharacteristicSubgraphs;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.functions.utils.AddCount;
import org.gradoop.flink.model.impl.operators.aggregation.ApplyAggregation;
import org.gradoop.flink.model.impl.operators.aggregation.functions.containment.HasLabel;
import org.gradoop.flink.model.impl.operators.aggregation.functions.containment.HasVertexLabel;
import org.gradoop.flink.model.impl.operators.subgraph.ApplySubgraph;
import org.gradoop.flink.model.impl.operators.subgraph.functions.LabelIsIn;
import org.gradoop.flink.model.impl.operators.transformation.ApplyTransformation;
import org.gradoop.flink.model.impl.operators.transformation.functions.Keep;
import org.gradoop.flink.model.impl.operators.transformation.functions.KeepAndSetProperty;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class CategoryCharacteristicSubgraphsTest extends GradoopFlinkTestBase {
  @Test
  public void execute() throws Exception {

    getExecutionEnvironment().setParallelism(1);

    GraphTransactions transactions = new PredictableTransactionsGenerator(
      100, 1, true, getConfig()).execute();

    GraphCollection collection = GraphCollection.fromTransactions(transactions);

    HasLabel hasVertexLabelB = new HasVertexLabel("B");
    HasLabel hasVertexLabelC = new HasVertexLabel("C");
    HasLabel hasVertexLabelD = new HasVertexLabel("D");

    collection = collection
      .apply(new ApplyAggregation(hasVertexLabelB))
      .apply(new ApplyAggregation(hasVertexLabelC))
      .apply(new ApplyAggregation(hasVertexLabelD));

    GraphCollection bGraphs = collection
      .select(hasVertexLabelB)
      .difference(collection
        .select(hasVertexLabelD)
      );

    GraphCollection cGraphs = bGraphs.select(hasVertexLabelC);

    bGraphs = bGraphs.difference(cGraphs);
    bGraphs = bGraphs.difference(cGraphs);

    bGraphs = bGraphs
      .apply(new ApplySubgraph(new LabelIsIn<Vertex>("A", "B"), null))
      .apply(new ApplyTransformation(
        new KeepAndSetProperty<GraphHead>(
          CategoryCharacteristicSubgraphs.CATEGORY_KEY, "B"),
        new Keep<Vertex>(),
        new Keep<Edge>()
      ));

    cGraphs = cGraphs
      .apply(new ApplySubgraph(new LabelIsIn<Vertex>("A", "C"), null))
      .apply(new ApplyTransformation(
        new KeepAndSetProperty<GraphHead>(
          CategoryCharacteristicSubgraphs.CATEGORY_KEY, "C"),
        new Keep<Vertex>(),
        new Keep<Edge>()
      ));

    collection = bGraphs.union(cGraphs);

    FSMConfig fsmConfig = new FSMConfig(0.8f, true);

    collection = collection
      .callForCollection(new CategoryCharacteristicSubgraphs(fsmConfig, 2.0f));

    transactions = collection.toTransactions();

    List<WithCount<Tuple2<String, String>>> categoryLabels = transactions
      .getTransactions()
      .flatMap(new CategoryVertexLabels())
      .map(new AddCount<Tuple2<String, String>>())
      .groupBy(0)
      .sum(1)
      .collect();

    assertEquals(2, categoryLabels.size());

    for (WithCount<Tuple2<String, String>> x : categoryLabels) {
      assertEquals(PredictableTransactionsGenerator
        .containedDirectedFrequentSubgraphs(1.0f), x.getCount());
    }
  }

  private class CategoryVertexLabels
    implements FlatMapFunction<GraphTransaction, Tuple2<String, String>> {

    @Override
    public void flatMap(GraphTransaction value,
      Collector<Tuple2<String, String>> out) throws Exception {

      String category = value
        .getGraphHead()
        .getPropertyValue(CategoryCharacteristicSubgraphs.CATEGORY_KEY)
        .toString();

      Set<String> vertexLabels = Sets.newHashSet();

      for (Vertex vertex : value.getVertices()) {
        vertexLabels.add(vertex.getLabel());
      }

      for (String label : vertexLabels) {
        out.collect(new Tuple2<>(category, label));
      }
    }
  }
}