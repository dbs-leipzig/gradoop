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

package org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.functions;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.config.BroadcastNames;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.tuples.FilterMessage;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.tuples.RefinementMessage;


/**
 * Candidate selection for refinement phase
 *
 * FilterResultMessage,.. => RefinementCall,..
 * where FilterResultMessage.subgraph == RefinementCall.subgraph
 */
public class FrequentOrRefinementCandidate
  extends RichGroupReduceFunction<FilterMessage, RefinementMessage> {

  /**
   * minimum support
   */
  private final float minSupport;
  /**
   * minimum frequency
   */
  private Integer minFrequency;
  /**
   * workerId - graph count map
   */
  private Map<Integer, Integer> workerGraphCount;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public FrequentOrRefinementCandidate(FSMConfig fsmConfig) {
    this.minSupport = fsmConfig.getMinSupport();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.minFrequency = getRuntimeContext()
      .<Integer>getBroadcastVariable(
        BroadcastNames.MIN_FREQUENCY).get(0);

    this.workerGraphCount = getRuntimeContext()
      .<Map<Integer, Integer>>getBroadcastVariable(
        BroadcastNames.WORKER_GRAPHCOUNT).get(0);
  }

  @Override
  public void reduce(Iterable<FilterMessage> messages,
    Collector<RefinementMessage> collector) throws Exception {

    // copy list of all workers
    Collection<Integer> workerIdsWithoutReport =
      Lists.newLinkedList(workerGraphCount.keySet());

    // evaluate first message
    Iterator<FilterMessage> iterator = messages.iterator();
    FilterMessage message = iterator.next();

    CompressedDFSCode subgraph = message.getSubgraph();
    workerIdsWithoutReport.remove(message.getWorkerId());

    int support = message.getFrequency();

    boolean atLeastOnceLocallyFrequent = message.isLocallyFrequent();

    // for each worker report
    while (iterator.hasNext()) {
      message = iterator.next();

      support += message.getFrequency();

      if (!atLeastOnceLocallyFrequent) {
        atLeastOnceLocallyFrequent = message.isLocallyFrequent();
      }

      workerIdsWithoutReport.remove(message.getWorkerId());
    }

    // CANDIDATE SELECTION

    if (atLeastOnceLocallyFrequent) {
      // support of all workers known
      if (workerIdsWithoutReport.isEmpty()) {
        // if globally frequent
        if (support >= minFrequency) {
          // emit complete support message
          collector.collect(new RefinementMessage(
            subgraph, support, RefinementMessage.COMPLETE_RESULT));
        }
      } else {
        int estimation = support;

        // add optimistic support estimations
        for (Integer workerId : workerIdsWithoutReport) {
          estimation += workerGraphCount.get(workerId) * minSupport;
        }
        // if likely globally frequent
        if (estimation >= minFrequency) {

          // emit incomplete support message
          collector.collect(new RefinementMessage(
            subgraph, support, RefinementMessage.INCOMPLETE_RESULT));

          // add refinement calls to missing workers
          for (Integer workerId : workerIdsWithoutReport) {
            collector.collect(new RefinementMessage(
              subgraph, workerId, RefinementMessage.REFINEMENT_CALL));
          }
        }
      }
    }
  }
}
