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

package org.gradoop.flink.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.config.TransactionalFSMAlgorithm;
import org.gradoop.flink.algorithms.fsm.gspan.api.GSpanEncoder;
import org.gradoop.flink.algorithms.fsm.gspan.api.GSpanMiner;
import org.gradoop.flink.algorithms.fsm.gspan.decoders.GSpanGraphCollectionDecoder;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.GSpanGraphCollectionEncoder;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.GSpanBulkIteration;
import org.gradoop.flink.algorithms.fsm.gspan.miners.filterrefine.GSpanFilterRefine;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 */
public class TransactionalFSM implements UnaryCollectionToCollectionOperator {

  /**
   * frequent subgraph mining configuration
   */
  protected final FSMConfig fsmConfig;
  /**
   * input encoder (pre processing)
   */
  private final GSpanEncoder<GraphCollection> encoder;
  /**
   * FSM implementation (actual algorithm)
   */
  private GSpanMiner miner;

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   * @param algorithm FSM implementation
   */
  public TransactionalFSM(FSMConfig fsmConfig, TransactionalFSMAlgorithm
    algorithm) {
    this.fsmConfig = fsmConfig;
    this.encoder = new GSpanGraphCollectionEncoder(fsmConfig) {
    };
    setMiner(algorithm);
  }

  @Override
  public GraphCollection execute(
    GraphCollection collection)  {

    miner.setExecutionEnvironment(
      collection.getConfig().getExecutionEnvironment());

    GSpanGraphCollectionDecoder decoder = new GSpanGraphCollectionDecoder(
      collection.getConfig());

    DataSet<GSpanGraph> graphs = encoder.encode(collection, fsmConfig);

    DataSet<WithCount<CompressedDFSCode>> frequentDfsCodes = miner
      .mine(graphs, encoder.getMinFrequency(), fsmConfig);

    return decoder.decode(
      frequentDfsCodes,
      encoder.getVertexLabelDictionary(),
      encoder.getEdgeLabelDictionary()
    );
  }

  /**
   * sets FSM implementation by a given enum
   * @param algorithm enum
   */
  private void setMiner(TransactionalFSMAlgorithm algorithm) {

    switch (algorithm) {
    case GSPAN_FILTERREFINE:
      miner = new GSpanFilterRefine();
      break;
    default:
      miner = new GSpanBulkIteration();
      break;
    }
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
