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

package org.gradoop.model.impl.algorithms.fsm.gspan.encoders;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.algorithms.fsm.config.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.gspan.api.GSpanEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.Dictionary;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.EdgeLabelsEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.EdgeLabels;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.InverseDictionary;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.MinFrequency;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.VertexLabelsEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.VertexLabels;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples.EdgeTripleWithStringEdgeLabel;
import org.gradoop.model.impl.algorithms.fsm.gspan.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.model.impl.functions.utils.AddCount;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.count.Count;

import java.util.Collection;
import java.util.List;
import java.util.Map;


/**
 * Transactional FSM pre processing: Determine vertex and edge label
 * frequencies, create frequency based dictionaries and finally translate und
 * filter vertices and edges
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GSpanGraphTransactionsEncoder
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements GSpanEncoder<GraphTransactions<G, V, E>> {

  /**
   * minimum support
   */
  private DataSet<Integer> minFrequency;

  /**
   * edge label dictionary
   */
  private DataSet<List<String>> edgeLabelDictionary;
  /**
   * vertex label dictionary
   */
  private DataSet<List<String>> vertexLabelDictionary;
  /**
   * FSM configuration
   */
  private final FSMConfig fsmConfig;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public GSpanGraphTransactionsEncoder(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  /**
   * determines edge label frequency and prunes by minimum frequency;
   * label frequencies are used to relabel edges where higher frequency leads
   * to a smaller numeric label;
   *
   * @param transactions input transactions
   * @param fsmConfig FSM configuration
   * @return pruned and relabelled edges
   */
  @Override
  public DataSet<GSpanGraph> encode(
    GraphTransactions<G, V, E> transactions, FSMConfig fsmConfig) {

    setMinFrequency(transactions, fsmConfig);

    DataSet<Collection<EdgeTripleWithStringEdgeLabel<GradoopId>>>
      triplesWithStringLabel = encodeVertices(transactions);

    return encodeEdges(triplesWithStringLabel);
  }

  /**
   * Calculates and stores minimum frequency
   * (minimum support * collection size).
   *
   * @param transactions input graph transactions
   * @param fsmConfig FSM configuration
   */
  private void setMinFrequency(
    GraphTransactions<G, V, E> transactions, FSMConfig fsmConfig) {

    this.minFrequency = Count
      .count(transactions.getTransactions())
      .map(new MinFrequency(fsmConfig));
  }

  /**
   * Determines edge label frequency, creates edge label dictionary,
   * filters edges by label frequency and translates edge labels
   *
   * @param tripleCollections input edges
   * @return translated and filtered edges
   */
  private DataSet<GSpanGraph> encodeEdges(
    DataSet<Collection<EdgeTripleWithStringEdgeLabel<GradoopId>>>
      tripleCollections) {

    edgeLabelDictionary = tripleCollections
      .flatMap(new EdgeLabels<GradoopId>())
      .map(new AddCount<String>())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<String>())
      .withBroadcastSet(minFrequency, BroadcastNames.MIN_FREQUENCY)
      .reduceGroup(new Dictionary());

    DataSet<Map<String, Integer>> reverseDictionary = edgeLabelDictionary
      .map(new InverseDictionary());

    return tripleCollections
      .map(new EdgeLabelsEncoder<GradoopId>(fsmConfig))
      .withBroadcastSet(reverseDictionary, BroadcastNames.EDGE_DICTIONARY);
  }

  /**
   * determines vertex label frequency and prunes by minimum frequency;
   * label frequencies are used to relabel vertices where higher frequency leads
   * to a smaller numeric label;
   *
   * @param transactions input graph collection
   * @return pruned and relabelled edges
   */
  private DataSet<Collection<EdgeTripleWithStringEdgeLabel<GradoopId>>>
  encodeVertices(GraphTransactions<G, V, E> transactions) {

    vertexLabelDictionary = transactions
      .getTransactions()
      .flatMap(new VertexLabels<G, V, E>())
      .map(new AddCount<String>())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<String>())
      .withBroadcastSet(minFrequency, BroadcastNames.MIN_FREQUENCY)
      .reduceGroup(new Dictionary());

    DataSet<Map<String, Integer>> reverseDictionary = vertexLabelDictionary
      .map(new InverseDictionary());

    return transactions
      .getTransactions()
      .map(new VertexLabelsEncoder<G, V, E>())
      .withBroadcastSet(reverseDictionary, BroadcastNames.VERTEX_DICTIONARY);
  }

  public DataSet<Integer> getMinFrequency() {
    return minFrequency;
  }

  @Override
  public DataSet<List<String>> getVertexLabelDictionary() {
    return vertexLabelDictionary;
  }

  @Override
  public DataSet<List<String>> getEdgeLabelDictionary() {
    return edgeLabelDictionary;
  }
}
