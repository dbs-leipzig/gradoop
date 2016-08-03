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

package org.gradoop.flink.datagen.transactions.predictable;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;
import java.util.Set;

/**
 * graphNumber => GraphTransaction
 */
public class PredictableTransaction implements
  MapFunction<Long, GraphTransaction>, ResultTypeQueryable<GraphTransaction> {

  /**
   * list of vertex labels
   */
  private static final List<String> VERTEX_LABELS = Lists
    .newArrayList("A", "B", "C", "D", "E", "F", "G", "H", "J", "K");
  /**
   * sets the minimum number of embeddings per subgraph pattern.
   */
  private final int graphSize;
  /**
   * sets the graph type: true => multigraph, false => simple graph
   */
  private final boolean multigraph;

  /**
   * graph head factory
   */
  private final GraphHeadFactory graphHeadFactory;
  /**
   * vertex factory
   */
  private final VertexFactory vertexFactory;
  /**
   * edge factory
   */
  private final EdgeFactory edgeFactory;

  /**
   * constructor
   *
   * @param graphSize minimum number of embeddings per subgraph pattern
   * @param multigraph multigraph mode
   * @param config Gradoop Flink configuration
   */
  PredictableTransaction(int graphSize, boolean multigraph,
    GradoopFlinkConfig config) {

    this.multigraph = multigraph;
    this.graphSize = graphSize;
    this.graphHeadFactory = config.getGraphHeadFactory();
    this.vertexFactory = config.getVertexFactory();
    this.edgeFactory = config.getEdgeFactory();
  }

  @Override
  public GraphTransaction map(Long graphNumber) throws Exception {

    Long maxVertexLabelIndex = graphNumber % 10;

    GraphHead graphHead = graphHeadFactory
      .createGraphHead(String.valueOf(maxVertexLabelIndex));

    Set<Vertex> vertices = Sets.newHashSet();
    Set<Edge> edges = Sets.newHashSet();

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());

    Vertex centerVertex = vertexFactory.createVertex("S", graphIds);
    vertices.add(centerVertex);

    for (int vertexLabelIndex = 0; vertexLabelIndex <= maxVertexLabelIndex;
         vertexLabelIndex++) {
      String vertexLabel = VERTEX_LABELS.get(vertexLabelIndex);

      for (int patternCopy = 1; patternCopy <= graphSize; patternCopy++) {
        addPattern(graphNumber, vertexLabel, centerVertex, vertices, edges);
      }
    }
    for (Vertex vertex : vertices) {
      vertex.setGraphIds(graphIds);
    }
    for (Edge edge : edges) {
      edge.setGraphIds(graphIds);
    }
    return new GraphTransaction(graphHead, vertices, edges);
  }

  /**
   * Adds a predictable pattern to the graph transaction. All vertices
   * will have a specified vertex label and the pattern will be connected to
   * the center vertex by an unique labelled edge.
   *
   * @param graphNumber numeric graph identifier
   * @param vertexLabel label of pattern vertices
   * @param centerVertex center vertex
   * @param vertices stores created vertices
   * @param edges stores created edges
   */
  private void addPattern(long graphNumber, String vertexLabel,
    Vertex centerVertex, Set<Vertex> vertices, Set<Edge> edges) {

    GradoopId multiBottomId = createVertex(vertexLabel, vertices);
    createEdge(
      centerVertex.getId(), String.valueOf(graphNumber), multiBottomId, edges);

    if (multigraph) {
      // parallel edges and loop
      GradoopId multiTopId = createVertex(vertexLabel, vertices);

      createEdge(multiBottomId, "p", multiTopId, edges);
      createEdge(multiTopId, "p", multiBottomId, edges);
      createEdge(multiTopId, "p", multiBottomId, edges);
      createEdge(multiTopId, "l", multiTopId, edges);
    }

    // mirror

    GradoopId mirrorBottomId = createVertex(vertexLabel, vertices);
    GradoopId mirrorTopId = createVertex(vertexLabel, vertices);
    GradoopId mirrorLeftId = createVertex(vertexLabel, vertices);
    GradoopId mirrorRightId = createVertex(vertexLabel, vertices);

    createEdge(mirrorBottomId, "m", mirrorLeftId, edges);
    createEdge(mirrorBottomId, "m", mirrorRightId, edges);
    createEdge(mirrorLeftId, "m", mirrorTopId, edges);
    createEdge(mirrorRightId, "m", mirrorTopId, edges);

    createEdge(mirrorBottomId, "s", multiBottomId, edges);

    // cycle

    GradoopId cycleBottomId = createVertex(vertexLabel, vertices);
    GradoopId cycleLeftId = createVertex(vertexLabel, vertices);
    GradoopId cycleRightId = createVertex(vertexLabel, vertices);

    createEdge(cycleBottomId, "c", cycleLeftId, edges);
    createEdge(cycleLeftId, "c", cycleRightId, edges);
    createEdge(cycleRightId, "c", cycleBottomId, edges);

    createEdge(cycleBottomId, "s", multiBottomId, edges);
  }

  /**
   * creates a vertex
   *
   * @param vertexLabel vertex label
   * @param vertices stores the vertex
   * @return id of the created vertex
   */
  private GradoopId createVertex(String vertexLabel, Set<Vertex> vertices) {

    Vertex vertex = vertexFactory.createVertex(vertexLabel);
    vertices.add(vertex);
    return vertex.getId();
  }

  /**
   * creates an edge
   *
   * @param sourceId source vertex id
   * @param label edge label
   * @param targetId target vertex id
   * @param edges stores the edge
   */
  private void createEdge(
    GradoopId sourceId, String label, GradoopId targetId, Set<Edge> edges) {

    edges.add(edgeFactory.createEdge(label, sourceId, targetId));
  }

  @Override
  public TypeInformation<GraphTransaction> getProducedType() {
    return new TupleTypeInfo<>(
      TypeExtractor.getForClass(graphHeadFactory.getType()),
      TypeExtractor.getForClass(Set.class),
      TypeExtractor.getForClass(Set.class));
  }
}
