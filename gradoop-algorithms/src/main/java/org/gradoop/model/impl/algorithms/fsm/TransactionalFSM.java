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

package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.config.TransactionalFSMAlgorithm;
import org.gradoop.model.impl.algorithms.fsm.decoders.gspan
  .GSpanGraphCollectionDecoder;
import org.gradoop.model.impl.algorithms.fsm.encoders.GraphCollectionTFSMEncoder;
import org.gradoop.model.impl.algorithms.fsm.encoders.TransactionalFSMEncoder;
import org.gradoop.model.impl.algorithms.fsm.encoders.tuples.EdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.miners.TransactionalFSMiner;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.bulkiteration
  .GSpanBulkIteration;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.common.pojos
  .CompressedDFSCode;
import org.gradoop.model.impl.algorithms.fsm.miners.gspan.filterrefine
  .GSpanFilterRefine;
import org.gradoop.model.impl.tuples.WithCount;

/**
 * abstract superclass of different implementations of the gSpan frequent
 * subgraph mining algorithm as Gradoop operator
 *
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class TransactionalFSM
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToCollectionOperator<G, V, E> {

  /**
   * frequent subgraph mining configuration
   */
  protected final FSMConfig fsmConfig;
  /**
   * input encoder (pre processing)
   */
  private final TransactionalFSMEncoder<GraphCollection<G, V, E>> encoder;
  /**
   * FSM implementation (actual algorithm)
   */
  private final TransactionalFSMiner miner;

  /**
   * constructor
   * @param fsmConfig frequent subgraph mining configuration
   * @param algorithm FSM implementation
   */
  public TransactionalFSM(FSMConfig fsmConfig, TransactionalFSMAlgorithm
    algorithm) {
    this.fsmConfig = fsmConfig;
    this.encoder = new GraphCollectionTFSMEncoder<>();
    this.miner = getMiner(algorithm);
  }

  @Override
  public GraphCollection<G, V, E> execute(
    GraphCollection<G, V, E> collection)  {

    miner.setExecutionEnvironment(
      collection.getConfig().getExecutionEnvironment());

    GSpanGraphCollectionDecoder<G, V, E> decoder =
      new GSpanGraphCollectionDecoder<>(collection.getConfig());

    DataSet<EdgeTriple> edges = encoder.encode(collection, fsmConfig);

    DataSet<WithCount<CompressedDFSCode>> frequentDfsCodes = miner
      .mine(edges, encoder.getMinFrequency(), fsmConfig);

    return decoder.decode(
      frequentDfsCodes,
      encoder.getVertexLabelDictionary(),
      encoder.getEdgeLabelDictionary()
    );
  }

  /**
   * returns FSM implementation by a given enum
   * @param algorithm enum
   * @return FSM implementation
   */
  private TransactionalFSMiner getMiner(TransactionalFSMAlgorithm algorithm) {
    TransactionalFSMiner algorithmMiner;

    switch (algorithm) {
    case GSPAN_FILTERREFINE:
      algorithmMiner = new GSpanFilterRefine();
      break;
    default:
      algorithmMiner = new GSpanBulkIteration();
      break;
    }

    return algorithmMiner;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
