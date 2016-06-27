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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.io.impl.tlf.tuples.TLFEdge;
import org.gradoop.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.io.impl.tlf.tuples.TLFGraphHead;
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
public class TLFGraphTransactions
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements FlatMapFunction<Tuple2<LongWritable, Text>,
  GraphTransaction<G, V, E>> {

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
  public TLFGraphTransactions(
    EPGMGraphHeadFactory<G> graphHeadFactory, EPGMVertexFactory<V>
    vertexFactory, EPGMEdgeFactory<E> edgeFactory) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;

    prepareForProducedType();
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
    Collection<TLFGraph> graphCollection = getGraphCollection(graphString);

    Set<V> vertices = Sets.newHashSet();
    Set<E> edges = Sets.newHashSet();

    GradoopId id;
    String label;
    GradoopId targetId;
    GradoopIdSet graphs = new GradoopIdSet();
    Map<Integer, GradoopId> integerGradoopIdMapVertices;

    for (TLFGraph graph : graphCollection) {
      integerGradoopIdMapVertices = Maps.newHashMap();
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

  /**
   * In order to return the produced type one GraphTransaction has to be
   * initiated.
   */
  private void prepareForProducedType() {
    Set<V> vertices = Sets.newHashSetWithExpectedSize(2);
    Set<E> edges = Sets.newHashSetWithExpectedSize(1);
    V source = this.vertexFactory.createVertex();
    V target = this.vertexFactory.createVertex();
    vertices.add(source);
    vertices.add(target);
    edges.add(this.edgeFactory.createEdge(source.getId(), target.getId()));

    graphTransaction = new GraphTransaction<G, V, E>(this
      .graphHeadFactory.initGraphHead(GradoopId.get()), vertices, edges);
  }

  /**
   * Creates, or returns if already created, the collection of graphs as
   * tuples from content.
   *
   * @param content string representation of a TLF graph
   * @return Collection of graphs as tuples
   */
  private Collection<TLFGraph> getGraphCollection(String content) {
    if (content.isEmpty()) {
      return null;
    }
    Collection<TLFGraph> graphCollection = Sets.newHashSet();
    TLFGraphHead graphHead;
    Collection<TLFVertex> vertices = Sets.newHashSet();
    Collection<TLFEdge> edges = Sets.newHashSet();

    //remove first tag so that split does not have empty first element
    String[] contentArray = content.trim().replaceFirst("t # ", "").split("t " +
      "# ");

    for (int i = 0; i < contentArray.length; i++) {
      vertices.addAll(getVertices(contentArray[i]));
      edges.addAll(getEdges(contentArray[i]));
      graphHead = getGraphHead(contentArray[i]);

      graphCollection.add(new TLFGraph(graphHead, vertices,
        edges));

      vertices = Sets.newHashSet();
      edges = Sets.newHashSet();
    }
    return graphCollection;
  }

  /**
   * Reads the vertices of a complete TLF graph segment.
   *
   * @param content the TLF graph segment
   * @return collection of vertices as tuples
   */
  private Collection<TLFVertex> getVertices(String content) {
    Collection<TLFVertex> vertexCollection = Sets.newHashSet();
    String[] vertex;
    //-1 cause before e is \n
    content = content.substring(content.indexOf("v"), content.indexOf("e") - 1);
    String[] vertices = content.split("\n");

    for (int i = 0; i < vertices.length; i++) {
      vertex = vertices[i].split(" ");
      vertexCollection.add(new TLFVertex(Integer.parseInt(
        vertex[1].trim()), vertex[2].trim()));
    }
    return vertexCollection;
  }

  /**
   * Reads the edges of a complete TLF graph segment.
   *
   * @param content the TLF graph segment
   * @return collection of edges as tuples
   */
  private Collection<TLFEdge> getEdges(String content) {

    Collection<TLFEdge> edgeCollection = Sets.newHashSet();
    String[] edge;

    content = content.substring(content.indexOf("e"), content.length());
    String[] edges = content.split("\n");

    for (int i = 0; i < edges.length; i++) {
      edge = edges[i].split(" ");
      edgeCollection.add(new TLFEdge(Integer
        .parseInt(edge[1].trim()), Integer.parseInt(edge[2].trim()), edge[3]));
    }
    return edgeCollection;
  }

  /**
   * Returns the graph head defined in content.
   *
   * @param content containing current graph as string
   * @return returns the graph head defined in content
   */
  private TLFGraphHead getGraphHead(String content) {
    return new TLFGraphHead(Long.parseLong(content.substring(0, 1)));
  }
}
