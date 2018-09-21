/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.algorithms.fsm.transactional.ccp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.algorithms.fsm.transactional.CategoryCharacteristicSubgraphs;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.datagen.transactions.predictable.PredictableTransactionsGenerator;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.functions.utils.AddCount;
import org.gradoop.flink.model.impl.operators.aggregation.ApplyAggregation;
import org.gradoop.flink.model.impl.operators.aggregation.functions.containment.HasLabel;
import org.gradoop.flink.model.impl.operators.aggregation.functions.containment.HasVertexLabel;
import org.gradoop.flink.model.impl.operators.subgraph.ApplySubgraph;
import org.gradoop.flink.model.impl.operators.subgraph.functions.LabelIsIn;
import org.gradoop.flink.model.impl.operators.transformation.ApplyTransformation;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class CategoryCharacteristicSubgraphsTest extends GradoopFlinkTestBase {
  @Test
  @Ignore
  public void execute() throws Exception {

    DataSet<GraphTransaction> transactions = new PredictableTransactionsGenerator(
      100, 1, true, getConfig()).execute();

    GraphCollection collection = getConfig().getGraphCollectionFactory()
      .fromTransactions(transactions);

    HasLabel hasVertexLabelB = new HasVertexLabel("B");
    HasLabel hasVertexLabelC = new HasVertexLabel("C");
    HasLabel hasVertexLabelD = new HasVertexLabel("D");

    collection = collection.apply(new ApplyAggregation(hasVertexLabelB, hasVertexLabelC,
      hasVertexLabelD));

    GraphCollection bGraphs = collection
      .select(hasVertexLabelB)
      .difference(collection
        .select(hasVertexLabelD)
      );

    GraphCollection cGraphs = bGraphs.select(hasVertexLabelC);

    bGraphs = bGraphs.difference(cGraphs);

    bGraphs = bGraphs
      .apply(new ApplySubgraph(new LabelIsIn<>("A", "B"), null))
      .apply(new ApplyTransformation(
        (current, transformed) -> {
          current.setProperty(CategoryCharacteristicSubgraphs.CATEGORY_KEY, "B");
          return current;
        },
        TransformationFunction.keep(),
        TransformationFunction.keep()
      ));

    cGraphs = cGraphs
      .apply(new ApplySubgraph(new LabelIsIn<>("A", "C"), null))
      .apply(new ApplyTransformation(
        (current, transformed) -> {
          current.setProperty(CategoryCharacteristicSubgraphs.CATEGORY_KEY, "C");
          return current;
        },
        TransformationFunction.keep(),
        TransformationFunction.keep()
      ));

    collection = bGraphs.union(cGraphs);

    FSMConfig fsmConfig = new FSMConfig(0.6f, true);

    collection = collection
      .callForCollection(new CategoryCharacteristicSubgraphs(fsmConfig, 2.0f));

    transactions = collection.getGraphTransactions();

    List<WithCount<Tuple2<String, String>>> categoryLabels = transactions
      .flatMap(new CategoryVertexLabels())
      .map(new AddCount<>())
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
    public void flatMap(GraphTransaction transaction,
      Collector<Tuple2<String, String>> out) throws Exception {

      final String category = transaction
        .getGraphHead()
        .getPropertyValue(CategoryCharacteristicSubgraphs.CATEGORY_KEY)
        .toString();

      transaction.getVertices().stream()
        .map(Element::getLabel)
        .distinct()
        .forEach(e -> out.collect(new Tuple2<>(category, e)));
    }
  }
}