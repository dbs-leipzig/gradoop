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

package org.gradoop.io.impl.tlf.functions;

import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.io.impl.tlf.tuples.TLFEdge;
import org.gradoop.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.io.impl.tlf.tuples.TLFVertex;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Reads graph imported from a TLF file. The result of the mapping is a
 * dataset of of graph transactions, with each GraphTransaction consisting
 * of a graph head, a set of vertices and a set of edges.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class TLFGraphCollectionToGraphTransactions<G extends
  EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> implements
  FlatMapFunction<Tuple2<LongWritable, Text>, GraphTransaction<G, V, E>> {

  /**
   * Creates graph data objects
   */
  private final EPGMGraphHeadFactory<G> graphHeadFactory;

  /**
   * Creates graph data objects
   */
  private final EPGMVertexFactory<V> vertexFactory;

  /**
   * Creates graph data objects
   */
  private final EPGMEdgeFactory<E> edgeFactory;

  /**
   * Reduce object instantiation.
   */
  private GraphTransaction<G, V, E> graphTransaction;

  /**
   * Creates a flatmap function.
   *
   * @param graphHeadFactory graph head data factory
   * @param vertexFactory    vertex data factory
   * @param edgeFactory      edge data factory
   */
  public TLFGraphCollectionToGraphTransactions(
    EPGMGraphHeadFactory<G> graphHeadFactory, EPGMVertexFactory<V>
    vertexFactory, EPGMEdgeFactory<E> edgeFactory) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;

    //the following is needed to be able to return the produced type
    Set<V> vertices = new HashSet<>();
    Set<E> edges = new HashSet<>();
    V source = this.vertexFactory.createVertex();
    V target = this.vertexFactory.createVertex();
    vertices.add(source);
    vertices.add(target);
    edges.add(this.edgeFactory.createEdge(source.getId(), target.getId()));

    graphTransaction = new GraphTransaction<G, V, E>(this
      .graphHeadFactory.initGraphHead(GradoopId.get()), vertices, edges);
  }

  /**
   * Constructs a dataset containing GraphTransaction(s).
   *
   * @param inputTuple consists of a key(LongWritable) and a value(Text)
   * @param collector of GraphTransaction(s)
   * @throws Exception
   */
  @Override
  public void flatMap(Tuple2<LongWritable, Text> inputTuple,
    Collector<GraphTransaction<G, V, E>> collector) throws Exception {
    String graphString = inputTuple.getField(1).toString();
    TLFStringToTLFGraphCollection
      tlfStringToCollection = new TLFStringToTLFGraphCollection();
    tlfStringToCollection.setContent(graphString);

    Collection<TLFGraph> graphCollection;
    graphCollection = tlfStringToCollection.getGraphCollection();

    Set<V> vertices = new HashSet<>();
    Set<E> edges = new HashSet<>();

    GradoopId id;
    String label;
    GradoopId targetId;
    GradoopIdSet graphs = new GradoopIdSet();
    Map<Integer, GradoopId> integerGradoopIdMapVertices;

    for (TLFGraph graph : graphCollection) {
      integerGradoopIdMapVertices = new HashedMap();
      graphs.clear();
      vertices.clear();
      edges.clear();

      id = GradoopId.get();
      graphs.add(id);

      for (TLFVertex vertex : graph.getGraphVertices()) {
        id = GradoopId.get();
        integerGradoopIdMapVertices.put(vertex.getId(), id);
        label = vertex.getLabel();
        vertices.add(this.vertexFactory.initVertex(id, label, graphs));
      }

      for (TLFEdge edge :graph.getGraphEdges()) {
        id = integerGradoopIdMapVertices.get(edge.getSourceId());
        targetId = integerGradoopIdMapVertices.get(edge.getTargetId());
        label = edge.getLabel();
        edges.add(this.edgeFactory.createEdge(label, id, targetId, graphs));
      }
      collector.collect(new GraphTransaction<G, V, E>(this
        .graphHeadFactory.initGraphHead(id), vertices, edges));
    }
  }

  /**
   * Returns the produced type information (GraphTransaction<G, V, E>) of the
   * flatmap.
   *
   * @return type information of GraphTransaction<G, V, E>
   */
  public TypeInformation<GraphTransaction<G, V, E>> getProducedType() {
    return TypeExtractor.getForObject(this.graphTransaction);
  }
}
