/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.functions.HasGrownSubgraphs;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.functions.IsCollector;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.functions.IterationItemWithCollector;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.pojos.IterationItem;

import org.gradoop.flink.algorithms.fsm.gspan.miners.GSpanBase;
import org.gradoop.flink.algorithms.fsm.config.BroadcastNames;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.functions.GrowFrequentSubgraphs;

import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.functions.IsTransaction;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.functions.IterationItemWithTransaction;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.functions.SubgraphCollection;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.functions.ExpandSubgraphs;
import org.gradoop.flink.algorithms.fsm.gspan.functions.Frequent;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.functions.PostPruneAndCompress;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.functions.ReportGrownSubgraphs;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.SerializedDFSCode;

import org.gradoop.flink.model.impl.functions.utils.AddCount;
import org.gradoop.flink.model.impl.functions.utils.SumCount;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Collection;

/**
 * gSpan implementation based on Flink Bulk Iteration.
 */
public class GSpanBulkIteration extends GSpanBase {

  @Override
  public DataSet<WithCount<CompressedDFSCode>> mine(
    DataSet<GSpanGraph> graphs,
    DataSet<Integer> minFrequency,
    FSMConfig fsmConfig) {

    DataSet<IterationItem> transactions = graphs
      .map(new IterationItemWithTransaction());
//      .map(new Print<IterationItem>(""));

    // create search space with collector

    Collection<WithCount<CompressedDFSCode>> emptySubgraphList =
      Lists.newArrayListWithExpectedSize(0);

    DataSet<IterationItem> searchSpace = transactions
      .union(getExecutionEnvironment()
        .fromElements(emptySubgraphList)
        .map(new IterationItemWithCollector())
        .returns(TypeInformation.of(IterationItem.class)));

    // ITERATION HEAD
    IterativeDataSet<IterationItem> workSet = searchSpace
      .iterate(fsmConfig.getMaxEdgeCount());

    // ITERATION BODY

    // determine grown frequent subgraphs
    transactions = workSet
      .filter(new IsTransaction());

    // report ,filter and validate frequent subgraphs
    DataSet<WithCount<CompressedDFSCode>> currentFrequentSubgraphs =
      transactions
        .flatMap(new ReportGrownSubgraphs())
        .map(new AddCount<SerializedDFSCode>())
        // count frequency per worker, prune and compress subgraph
        .groupBy(0)
        .combineGroup(new SumCount<SerializedDFSCode>())
        .flatMap(new PostPruneAndCompress(fsmConfig))
        // count global frequency and filter frequent subgraphs
        .groupBy(0)
        .sum(1)
        .filter(new Frequent<CompressedDFSCode>())
        .withBroadcastSet(minFrequency, BroadcastNames.MIN_FREQUENCY);

    // get all frequent subgraphs
    DataSet<Collection<WithCount<CompressedDFSCode>>> collector = workSet
      .filter(new IsCollector())
      .map(new SubgraphCollection())
      .union(
        currentFrequentSubgraphs
          .reduceGroup(new SubgraphCollection(fsmConfig))
      )
      .reduce(new SubgraphCollection());

    // grow frequent subgraphs
    DataSet<IterationItem> nextWorkSet = transactions
      .map(new GrowFrequentSubgraphs(fsmConfig))
      .withBroadcastSet(
        currentFrequentSubgraphs, BroadcastNames.FREQUENT_SUBGRAPHS)
      .filter(new HasGrownSubgraphs())
      .union(
        collector
        .map(new IterationItemWithCollector())
      );

    // ITERATION FOOTER
    DataSet<IterationItem> resultSet = workSet
      // terminate, if no new frequent DFS patterns
      .closeWith(nextWorkSet, currentFrequentSubgraphs);

    return resultSet
      .filter(new IsCollector())
      .flatMap(new ExpandSubgraphs());
  }

}
