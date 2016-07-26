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
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.config.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.config.FSMConfig;
import org.gradoop.model.impl.algorithms.fsm.gspan.api.GSpanEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.AppendSourceLabel;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.AppendTargetLabel;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.Dictionary;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.EdgeLabelEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.GraphIdElementIdLabel;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.InverseDictionary;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.MinFrequency;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions.VertexLabelEncoder;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples.EdgeTripleWithoutVertexLabels;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples.VertexIdLabel;
import org.gradoop.model.impl.algorithms.fsm.gspan.functions.CombineGSpanGraph;
import org.gradoop.model.impl.algorithms.fsm.gspan.functions.Frequent;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.model.impl.functions.tuple.Value1Of2;
import org.gradoop.model.impl.functions.utils.AddCount;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.count.Count;

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
public class GSpanGraphCollectionEncoder
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements GSpanEncoder<GraphCollection<G, V, E>> {

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
  public GSpanGraphCollectionEncoder(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  /**
   * determines edge label frequency and prunes by minimum frequency;
   * label frequencies are used to relabel edges where higher frequency leads
   * to a smaller numeric label;
   *
   * @param collection input collection
   * @param fsmConfig FSM configuration
   * @return pruned and relabelled edges
   */
  @Override
  public DataSet<GSpanGraph> encode(
    GraphCollection<G, V, E> collection, FSMConfig fsmConfig) {

    setMinFrequency(collection, fsmConfig);

    DataSet<VertexIdLabel> encodedVertices =
      encodeVertices(collection.getVertices());

    DataSet<EdgeTripleWithoutVertexLabels> encodedEdges =
      encodeEdges(collection.getEdges());

    return combine(encodedVertices, encodedEdges);
  }

  /**
   * Calculates and stores minimum frequency
   * (minimum support * collection size).
   *
   * @param collection input graph collection
   * @param fsmConfig FSM configuration
   */
  private void setMinFrequency(
    GraphCollection<G, V, E> collection, FSMConfig fsmConfig) {

    this.minFrequency = Count
      .count(collection.getGraphHeads())
      .map(new MinFrequency(fsmConfig));
  }

  /**
   * Determines edge label frequency, creates edge label dictionary,
   * filters edges by label frequency and translates edge labels
   *
   * @param edges input edges
   * @return translated and filtered edges
   */
  private DataSet<EdgeTripleWithoutVertexLabels> encodeEdges(
    DataSet<E> edges) {

    edgeLabelDictionary = edges
      .flatMap(new GraphIdElementIdLabel<E>())
      .distinct()
      .map(new Value1Of2<GradoopId, String>())
      .map(new AddCount<String>())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<String>())
      .withBroadcastSet(minFrequency, BroadcastNames.MIN_FREQUENCY)
      .reduceGroup(new Dictionary());

    DataSet<Map<String, Integer>> reverseDictionary = edgeLabelDictionary
      .map(new InverseDictionary());

    return edges
      .flatMap(new EdgeLabelEncoder<E>())
      .withBroadcastSet(reverseDictionary, BroadcastNames.EDGE_DICTIONARY);
  }

  /**
   * determines vertex label frequency and prunes by minimum frequency;
   * label frequencies are used to relabel vertices where higher frequency leads
   * to a smaller numeric label;
   *
   * @param vertices input vertex collection
   * @return pruned and relabelled edges
   */
  private DataSet<VertexIdLabel> encodeVertices(DataSet<V> vertices) {

    vertexLabelDictionary = vertices
      .flatMap(new GraphIdElementIdLabel<V>())
      .distinct()
      .map(new Value1Of2<GradoopId, String>())
      .map(new AddCount<String>())
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<String>())
      .withBroadcastSet(minFrequency, BroadcastNames.MIN_FREQUENCY)
      .reduceGroup(new Dictionary());

    DataSet<Map<String, Integer>> reverseDictionary = vertexLabelDictionary
      .map(new InverseDictionary());

    return vertices
      .flatMap(new VertexLabelEncoder<V>())
      .withBroadcastSet(reverseDictionary, BroadcastNames.VERTEX_DICTIONARY);
  }

  /**
   * Combines encoded vertices and encoded edges to edge triples.
   *
   * @param encodedVertices vertices
   * @param encodedEdges edges
   * @return triples
   */
  private DataSet<GSpanGraph> combine(DataSet<VertexIdLabel> encodedVertices,
    DataSet<EdgeTripleWithoutVertexLabels> encodedEdges) {

    return encodedEdges
      .join(encodedVertices).where(1).equalTo(0)
      .with(new AppendSourceLabel())
      .join(encodedVertices).where(2).equalTo(0)
      .with(new AppendTargetLabel())
      .groupBy(0)
      .reduceGroup(new CombineGSpanGraph(fsmConfig));
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
