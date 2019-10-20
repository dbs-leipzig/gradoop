/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/**
 * Prune (and reinsert) vertices of degree 1 to simplify the layouting
 */
public class GiLaDegreePruner {

  /**
   * Property name for the amount of pruned neighbors of a vertex
   */
  public static final String NUM_PRUNED_NEIGHBORS_PROPERTY = "NUM_PRUNED_NEIGHBORS";
  /**
   * Property name for the length of the shortest edge of the vertex
   */
  private static final String MIN_EDGE_LEN_PROPERTY = "MIN_EDGE_LEN";
  /**
   * Fraction of the shortest edge length that will be the distance for reinserted vertices
   */
  private static final double REINSERT_EDGE_LENGHT_FRACTION = 0.2;

  /**
   * Vertices pruned from the graph
   */
  private DataSet<EPGMVertex> prunedVertices;
  /**
   * Edges pruned from the graph
   */
  private DataSet<EPGMEdge> prunedEdges;
  /**
   * Original (non-pruned) edges of the graph
   */
  private DataSet<EPGMEdge> originalEdges;

  /**
   * Remove all vertices of degree 1 from the graph. Some vertices will receive
   * a property NUM_PRUNED_NEIGHBORS containing the number of neighbors that were pruned
   *
   * @param g The graph to prune
   * @return A graph like g but without vertices of degree 1
   */
  public LogicalGraph prune(LogicalGraph g) {
    int minDegree = 2;
    DataSet<EPGMVertex> vertices = g.getVertices();
    DataSet<EPGMEdge> edges = g.getEdges();

    originalEdges = edges;

    DataSet<Tuple2<GradoopId, Integer>> degrees = calculateDegrees(edges);

    DataSet<Tuple2<GradoopId, Integer>> toRemove = degrees.filter(d -> d.f1 < minDegree);

    prunedVertices = filterVertices(vertices, toRemove, false);
    DataSet<EPGMVertex> newVertices = filterVertices(vertices, toRemove, true);

    DataSet<EPGMEdge> newEdges = filterEdges(edges, toRemove, "sourceId", true);
    newEdges = filterEdges(newEdges, toRemove, "targetId", true);

    prunedEdges = filterEdges(edges, toRemove, "sourceId", false)
      .union(filterEdges(edges, toRemove, "targetId", false));

    newVertices = annotateNumPrunedNeighbors(newVertices);

    return g.getFactory().fromDataSets(newVertices, newEdges);
  }

  /**
   * Reinsert previously pruned Vertices into the graph. Place them near their neighbors.
   *
   * @param graph The pruned AND LAYOUTED! graph
   * @return g with vertices of degree 1 reinserted
   */
  public LogicalGraph reinsert(LogicalGraph graph) {
    DataSet<EPGMVertex> vertices = graph.getVertices();
    DataSet<EPGMEdge> edges = graph.getEdges();

    vertices = annotateShortestEdge(vertices, edges);
    prunedVertices = positionPrunedVertices(vertices);

    return graph.getFactory().fromDataSets(vertices.union(prunedVertices), originalEdges);
  }

  /**
   * Adds a property NUM_PRUNED_NEIGHBORS to all vertices containing the number of pruned neighbors
   *
   * @param vertices Input vertices
   * @return Output vertices
   */
  private DataSet<EPGMVertex> annotateNumPrunedNeighbors(DataSet<EPGMVertex> vertices) {
    return vertices.leftOuterJoin(makeUndirected(prunedEdges)).where("id").equalTo("sourceId")
      .with(new JoinFunction<EPGMVertex, EPGMEdge, Tuple2<EPGMVertex, Integer>>() {
        @Override
        public Tuple2<EPGMVertex, Integer> join(EPGMVertex vertex, EPGMEdge edge) throws Exception {
          return new Tuple2<>(vertex, (edge == null) ? 0 : 1);
        }
      }).groupBy("f0.id").reduce((a, b) -> {
        a.f1 += b.f1;
        return a;
      }).map((MapFunction<Tuple2<EPGMVertex, Integer>, EPGMVertex>) (t) -> {
        t.f0.setProperty(NUM_PRUNED_NEIGHBORS_PROPERTY, t.f1);
        return t.f0;
      });
  }

  /**
   * Given the remaining (non-pruned) vertices, place the previously pruned vertices close to them.
   * (Assing similar but randomly offseted coordinates)
   *
   * @param remainingVertices The non-pruned vertices of the graph
   * @return The pruned vertices with X and Y coordinate properties
   */
  private DataSet<EPGMVertex> positionPrunedVertices(DataSet<EPGMVertex> remainingVertices) {
    return prunedVertices.join(makeUndirected(prunedEdges)).where("id").equalTo("sourceId")
      .join(remainingVertices).where("f1.targetId").equalTo("id")
      .with(new JoinFunction<Tuple2<EPGMVertex, EPGMEdge>, EPGMVertex, EPGMVertex>() {
        @Override
        public EPGMVertex join(Tuple2<EPGMVertex, EPGMEdge> vertexEdgeTuple2,
          EPGMVertex remainingVertex) {
          double minEdgeLen = remainingVertex.getPropertyValue(MIN_EDGE_LEN_PROPERTY).getDouble();
          double distance = minEdgeLen * REINSERT_EDGE_LENGHT_FRACTION;
          Vector offset = new Vector(distance, 0);
          offset.mRotate(Math.random() * 360);
          Vector origPosition = Vector.fromVertexPosition(remainingVertex);
          offset.mAdd(origPosition);
          offset.setVertexPosition(vertexEdgeTuple2.f0);
          return vertexEdgeTuple2.f0;
        }
      });
  }

  /**
   * Make edges undirected by copying the edges and swapping source and target for the copies
   *
   * @param edges The directed edges
   * @return Undirected edges
   */
  private DataSet<EPGMEdge> makeUndirected(DataSet<EPGMEdge> edges) {
    return edges.flatMap(new FlatMapFunction<EPGMEdge, EPGMEdge>() {
      @Override
      public void flatMap(EPGMEdge e, Collector<EPGMEdge> collector) throws Exception {
        EPGMEdge edgeCopy = new EPGMEdge(GradoopId.get(), e.getLabel(), e.getTargetId(), e.getSourceId(),
          new Properties(), e.getGraphIds());
        collector.collect(e);
        collector.collect(edgeCopy);
      }
    });
  }

  /**
   * Add a property MIN_EDGE_LEN containing the length of the shortest edge for this vertex
   *
   * @param vertices The non-pruned vertices
   * @param edges    The non-pruned edges
   * @return The non-pruned vertices, but some now have a property MIN_EDGE_LEN
   */
  private DataSet<EPGMVertex> annotateShortestEdge(DataSet<EPGMVertex> vertices, DataSet<EPGMEdge> edges) {
    return vertices.join(edges).where("id").equalTo("sourceId").join(vertices)
      .where("f1" + ".targetId").equalTo("id")
      .with(new FlatJoinFunction<Tuple2<EPGMVertex, EPGMEdge>, EPGMVertex, EPGMVertex>() {
        @Override
        public void join(Tuple2<EPGMVertex, EPGMEdge> vertexEdgeTuple2, EPGMVertex vertex,
          Collector<EPGMVertex> collector) throws Exception {
          Vector pos1 = Vector.fromVertexPosition(vertexEdgeTuple2.f0);
          Vector pos2 = Vector.fromVertexPosition(vertex);
          double len = pos1.distance(pos2);
          vertexEdgeTuple2.f0.setProperty(MIN_EDGE_LEN_PROPERTY, len);
          vertex.setProperty(MIN_EDGE_LEN_PROPERTY, len);
          collector.collect(vertexEdgeTuple2.f0);
          collector.collect(vertex);
        }
      }).groupBy("id").reduce((v1, v2) -> {
        if (v1.getPropertyValue(MIN_EDGE_LEN_PROPERTY).getDouble() <
          v2.getPropertyValue(MIN_EDGE_LEN_PROPERTY).getDouble()) {
          return v1;
        }
        return v2;
      });
  }

  /**
   * Filter edges based on the pruning criterium
   *
   * @param edges    The original edges
   * @param toRemove Dataset containing ids of vertices to prune
   * @param field    choose to filter by targetId oc sourceId
   * @param keep     If true, return non-pruned edges, if false return pruned edges
   * @return Some edges (depending on keep)
   */
  private DataSet<EPGMEdge> filterEdges(DataSet<EPGMEdge> edges,
    DataSet<Tuple2<GradoopId, Integer>> toRemove, String field, boolean keep) {
    return edges.leftOuterJoin(toRemove).where(field).equalTo(0)
      .with(new FlatJoinFunction<EPGMEdge, Tuple2<GradoopId, Integer>, EPGMEdge>() {
        @Override
        public void join(EPGMEdge edge, Tuple2<GradoopId, Integer> remove, Collector<EPGMEdge> collector) {
          if ((remove == null) == keep) {
            collector.collect(edge);
          }
        }
      });
  }

  /**
   * Calculcates the edge-degree for a vertex
   *
   * @param edges Original edges
   * @return Tuples containing vertexId and the calculated degree for the vertex
   */
  private DataSet<Tuple2<GradoopId, Integer>> calculateDegrees(DataSet<EPGMEdge> edges) {
    return edges.flatMap((FlatMapFunction<EPGMEdge, Tuple2<GradoopId, Integer>>) (e, collector) -> {
      collector.collect(new Tuple2<>(e.getSourceId(), 1));
      collector.collect(new Tuple2<>(e.getTargetId(), 1));
    }).returns(new TypeHint<Tuple2<GradoopId, Integer>>() {
    }).groupBy(0).reduce((a, b) -> {
      a.f1 += b.f1;
      return a;
    });
  }

  /**
   * Filter vertices based on the pruning criterium
   *
   * @param vertices The original vertices
   * @param toRemove Dataset containing ids of vertices to prune
   * @param keep     If true, return non-pruned vertices, if false return pruned vertices
   * @return Some vertices (depending on keep)
   */
  private DataSet<EPGMVertex> filterVertices(DataSet<EPGMVertex> vertices,
    DataSet<Tuple2<GradoopId, Integer>> toRemove, boolean keep) {
    return vertices.leftOuterJoin(toRemove).where("id").equalTo(0)
      .with(new FlatJoinFunction<EPGMVertex, Tuple2<GradoopId, Integer>, EPGMVertex>() {
        @Override
        public void join(EPGMVertex vertex, Tuple2<GradoopId, Integer> remove,
          Collector<EPGMVertex> collector) {
          if ((remove == null) == keep) {
            collector.collect(vertex);
          }
        }
      });
  }

}
