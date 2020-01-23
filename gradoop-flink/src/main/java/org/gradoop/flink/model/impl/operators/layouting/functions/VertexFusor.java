/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LGraph;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.Random;

/**
 * Simplifies the graph by combining similar vertices to super-vertices using a
 * Comparison-Function and a threshold.
 */
public class VertexFusor {
  /**
   * The VertexCompareFunction to use to find similar vertices
   */
  protected VertexCompareFunction compareFunction;
  /**
   * Only consider vertices as similar if their similarity is larger then this value
   */
  protected double threshold;

  /**
   * Construct new VertexFusor
   *
   * @param compareFunction The VertexCompareFunction to use to find similar vertices
   * @param threshold       Only consider vertices as similar if their similarity is larger then
   *                        this
   *                        value
   */
  public VertexFusor(VertexCompareFunction compareFunction, double threshold) {
    this.compareFunction = compareFunction;
    this.threshold = threshold;
  }

  /**
   * Execute the operation. Should be called iteratively. A single call is usually not enough for
   * practical results.
   *
   * @param graph The graph to simplify
   * @return A tuple containing the vertices and edges of the simplified graph
   */
  public LGraph execute(LGraph graph) {
    DataSet<LVertex> vertices = graph.getVertices();
    DataSet<LEdge> edges = graph.getEdges();

    DataSet<Tuple2<LVertex, Boolean>> classifiedVertices = chooseDonorsAndAcceptors(vertices);

    DataSet<Tuple2<LVertex, LVertex>> fusions = generateFusionCandidates(classifiedVertices, edges);

    DataSet<LVertex> superVertices = fusions.groupBy("1.0").reduceGroup(new SuperVertexGenerator());

    DataSet<LVertex> remainingVertices = findRemainingVertices(fusions, vertices, superVertices);

    vertices = remainingVertices.union(superVertices);

    edges = fixEdgeReferences(edges, fusions);

    edges = edges.groupBy(LEdge.SOURCE_ID_POSITION, LEdge.TARGET_ID_POSITION).reduce((a, b) -> {
      a.addSubEdge(b.getId());
      a.addSubEdges(b.getSubEdges());
      return a;
    });

    return new LGraph(vertices, edges);
  }

  /**
   * Splits the vertices randomly into donor and acceptor-vertices (~50/50).
   *
   * @param vertices vertices to classify
   * @return {@code Tuple2<LVertex, Boolean>}. If the boolean is true, the vertex is an acceptor
   */
  protected DataSet<Tuple2<LVertex, Boolean>> chooseDonorsAndAcceptors(DataSet<LVertex> vertices) {
    final Random rng = new Random();
    return vertices.map(v -> new Tuple2<>(v, rng.nextBoolean()))
      .returns(new TypeHint<Tuple2<LVertex, Boolean>>() {
      });
  }

  /**
   * Finds connected vertices by joining with the edges. Finds out if the two vertices should be
   * merged together and if so outputs a tuple for each merge.
   *
   * @param classifiedVertices The vertices (split into donors and acceptors)
   * @param edges              The edges
   * @return {@code Tuple2<LVertex, LVertex>} for each merge. f0 is the donor and f1 the acceptor
   * for the
   * merge.
   */
  protected DataSet<Tuple2<LVertex, LVertex>> generateFusionCandidates(
    DataSet<Tuple2<LVertex, Boolean>> classifiedVertices, DataSet<LEdge> edges) {
    return edges.join(classifiedVertices).where(LEdge.SOURCE_ID_POSITION).equalTo("0." + LVertex.ID_POSITION)
      .join(classifiedVertices).where("0." + LEdge.TARGET_ID_POSITION).equalTo("0." + LVertex.ID_POSITION)
      .with(new CandidateGenerator(compareFunction, threshold)).groupBy("0.0")
      .reduce((a, b) -> (a.f2 > b.f2) ? a : b).map(c -> new Tuple2<>(c.f0, c.f1))
      .returns(new TypeHint<Tuple2<LVertex, LVertex>>() { });
  }

  /**
   * There are some vertices that have neither become super-vertices nor have been merged with a
   * super vertex. Find them so they can be copied to the simplified graph.
   *
   * @param fusions       The merges that are to be performed in this iteration
   * @param vertices      The original vertices of the input graph
   * @param superVertices The newly created super-vertices
   * @return All vertices that have to be copied to the output-graph
   */
  protected DataSet<LVertex> findRemainingVertices(DataSet<Tuple2<LVertex, LVertex>> fusions,
    DataSet<LVertex> vertices, DataSet<LVertex> superVertices) {
    DataSet<LVertex> remainingVertices =
      vertices.leftOuterJoin(superVertices).where(LVertex.ID_POSITION).equalTo(LVertex.ID_POSITION)
        .with(new FlatJoinFunction<LVertex, LVertex, LVertex>() {
          @Override
          public void join(LVertex lVertex, LVertex lVertex2, Collector<LVertex> collector) {
            if (lVertex2 == null) {
              collector.collect(lVertex);
            }
          }
        });

    remainingVertices =
      remainingVertices.leftOuterJoin(fusions).where(LVertex.ID_POSITION).equalTo("0." + LVertex.ID_POSITION)
        .with(new FlatJoinFunction<LVertex, Tuple2<LVertex, LVertex>, LVertex>() {
          @Override
          public void join(LVertex lVertex, Tuple2<LVertex, LVertex> lVertexLVertexDoubleTuple3,
            Collector<LVertex> collector) {
            if (lVertexLVertexDoubleTuple3 == null) {
              collector.collect(lVertex);
            }
          }
        });
    return remainingVertices;
  }

  /**
   * When combining two vertices into one the edges of the old vertices have to be modified to
   * point to the new vertex.
   *
   * @param edges   The edges of the input-graph
   * @param fusions The merges performed in the current iteration
   * @return The "fixed" edges for the output-graph
   */
  protected DataSet<LEdge> fixEdgeReferences(DataSet<LEdge> edges,
    DataSet<Tuple2<LVertex, LVertex>> fusions) {
    edges = edges.leftOuterJoin(fusions).where(LEdge.SOURCE_ID_POSITION).equalTo("0." + LVertex.ID_POSITION)
      .with(new JoinFunction<LEdge, Tuple2<LVertex, LVertex>, LEdge>() {
        @Override
        public LEdge join(LEdge lEdge, Tuple2<LVertex, LVertex> lVertexLVertexDoubleTuple3) {
          if (lVertexLVertexDoubleTuple3 != null) {
            lEdge.setSourceId(lVertexLVertexDoubleTuple3.f1.getId());
          }
          return lEdge;
        }
      });

    edges = edges.leftOuterJoin(fusions).where(LEdge.TARGET_ID_POSITION).equalTo("0." + LVertex.ID_POSITION)
      .with(new JoinFunction<LEdge, Tuple2<LVertex, LVertex>, LEdge>() {
        @Override
        public LEdge join(LEdge lEdge, Tuple2<LVertex, LVertex> lVertexLVertexDoubleTuple3) {
          if (lVertexLVertexDoubleTuple3 != null) {
            lEdge.setTargetId(lVertexLVertexDoubleTuple3.f1.getId());
          }
          return lEdge;
        }
      });
    return edges;
  }

  /**
   * Finds out if two given vertices could be merged into one and how "good" this merge would be.
   * The strange signature of the join function is needed to be able to directly use it in the
   * join of generateFusionCandidates().
   */
  protected static class CandidateGenerator implements
    FlatJoinFunction<Tuple2<LEdge, Tuple2<LVertex, Boolean>>, Tuple2<LVertex, Boolean>,
      Tuple3<LVertex, LVertex, Double>> {

    /**
     * ComparisonFunction to use to compute similarity of vertices
     */
    protected VertexCompareFunction cf;
    /**
     * Minimum similarity to allow merge
     */
    protected Double threshold;

    /**
     * Construct new instance
     *
     * @param cf        ComparisonFunction to use to compute similarity of vertices
     * @param threshold Minimum similarity to allow merge
     */
    public CandidateGenerator(VertexCompareFunction cf, Double threshold) {
      this.cf = cf;
      this.threshold = threshold;
    }

    @Override
    public void join(Tuple2<LEdge, Tuple2<LVertex, Boolean>> source,
      Tuple2<LVertex, Boolean> target, Collector<Tuple3<LVertex, LVertex, Double>> collector) throws
      Exception {

      LVertex sourceVertex = source.f1.f0;
      boolean sourceType = source.f1.f1;

      LVertex targetVertex = target.f0;
      boolean targetType = target.f1;

      // Can not merge two vertices of the same type (donor/acceptor)
      if (sourceType == targetType) {
        return;
      }

      Double similarity = cf.compare(sourceVertex, targetVertex);

      // Can not merge vertices that are not similar enough
      if (similarity < threshold) {
        return;
      }

      // The acceptor-vertex (targetType==true) MUST be the second element of the tuple
      if (targetType) {
        collector.collect(new Tuple3<>(sourceVertex, targetVertex, similarity));
      } else {
        collector.collect(new Tuple3<>(targetVertex, sourceVertex, similarity));
      }

    }
  }

  /**
   * Combines multiple vertices into a single super-vertex. The created super-vertex inherits the
   * id of the acceptor. The position of the super-vertex is a weighted average of all
   * participating vertices.
   */
  protected static class SuperVertexGenerator implements
    GroupReduceFunction<Tuple2<LVertex, LVertex>, LVertex> {

    /**
     * Combine vertices
     *
     * @param iterable  The vertices to combine. f0 contains donor-vertices and f1 contains
     *                  (always the same) acceptor vertex.
     * @param collector Collector for the resuls
     */
    @Override
    public void reduce(Iterable<Tuple2<LVertex, LVertex>> iterable,
      Collector<LVertex> collector) {
      int count = 0;
      Vector positionSum = new Vector();
      LVertex self = null;

      for (Tuple2<LVertex, LVertex> t : iterable) {
        if (count == 0) {
          self = t.f1;
          count = t.f1.getCount();
          positionSum.mAdd(t.f1.getPosition().mul(t.f1.getCount()));
        }
        count += t.f0.getCount();
        positionSum.mAdd(t.f0.getPosition().mul(t.f0.getCount()));
        self.addSubVertex(t.f0.getId());
        self.addSubVertices(t.f0.getSubVertices());
      }

      self.setPosition(positionSum.div(count));

      collector.collect(self);
    }
  }
}
