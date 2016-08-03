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

package org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine;

import java.util.Collection;
import java.util.Map;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions.Partition;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions.RefinementCall;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions.RefinementCalls;

import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions
  .CompressedSubgraphWithCount;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.algorithms.fsm.config.BroadcastNames;
import org.gradoop.flink.algorithms.fsm.gspan.miners.GSpanBase;
import org.gradoop.flink.algorithms.fsm.gspan.functions.Frequent;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions.CompleteResult;


import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions.FrequentOrRefinementCandidate;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions.PartitionGSpan;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions.IncompleteResult;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions.Refinement;


import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions.WorkerIdGraphCount;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions.WorkerIdsGraphCounts;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.tuples.FilterMessage;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.tuples.RefinementMessage;

/**
 * gSpan implementation using the filter and refine approach (ICDE 2014).
 */
public class GSpanFilterRefine extends GSpanBase {

  @Override
  public DataSet<WithCount<CompressedDFSCode>> mine(
    DataSet<GSpanGraph> graphs,
    DataSet<Integer> minFrequency,
    FSMConfig fsmConfig) {

    // distribute graphs to workers
    DataSet<Tuple2<Integer, Collection<GSpanGraph>>> partitions = graphs
      .rebalance()
      .mapPartition(new Partition());

    // get worker ids with local graph counts
    DataSet<Map<Integer, Integer>> workerIdsGraphCount = partitions
      .map(new WorkerIdGraphCount())
      .reduceGroup(new WorkerIdsGraphCounts());

    // FILTER round
    DataSet<FilterMessage> fsmResult =
      partitions
        // run local FSM
        .flatMap(new PartitionGSpan(fsmConfig));

    DataSet<RefinementMessage> filterResult = fsmResult
      // group reports by DFS code
      .groupBy(0)
      // keep if sure or likely globally frequent; drop otherwise
      .reduceGroup(new FrequentOrRefinementCandidate(fsmConfig))
      .withBroadcastSet(minFrequency, BroadcastNames.MIN_FREQUENCY)
      .withBroadcastSet(workerIdsGraphCount, BroadcastNames.WORKER_GRAPHCOUNT);

    // add globally frequent DFS codes to result
    DataSet<WithCount<CompressedDFSCode>> frequentDfsCodes = filterResult
      .filter(new CompleteResult())
      .map(new CompressedSubgraphWithCount());

    // REFINEMENT

    // remember incomplete results
    DataSet<WithCount<CompressedDFSCode>> partialResults = filterResult
      .filter(new IncompleteResult())
      .map(new CompressedSubgraphWithCount());

    // get refined results
    DataSet<WithCount<CompressedDFSCode>> refinementResults = filterResult
      .filter(new RefinementCall())
      .groupBy(1) // workerId
      .reduceGroup(new RefinementCalls())
      .join(partitions)
      .where(0).equalTo(0)
      .with(new Refinement(fsmConfig));

    frequentDfsCodes = frequentDfsCodes.union(
      partialResults
        .union(refinementResults)
        .groupBy(0)
        .sum(1)
        .filter(new Frequent<CompressedDFSCode>())
        .withBroadcastSet(minFrequency, BroadcastNames.MIN_FREQUENCY)
    );

    return frequentDfsCodes;
  }


}
