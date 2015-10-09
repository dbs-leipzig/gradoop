package org.gradoop.model.impl.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.functions.keyselectors.EdgeKeySelector;
import org.gradoop.model.impl.functions.keyselectors
  .EdgeSourceVertexKeySelector;
import org.gradoop.model.impl.functions.keyselectors
  .EdgeTargetVertexKeySelector;
import org.gradoop.model.impl.functions.keyselectors.GraphKeySelector;
import org.gradoop.model.impl.functions.keyselectors.VertexKeySelector;
import org.gradoop.model.impl.tuples.Subgraph;

/**
 * Base class for set operations that share common methods to build vertex,
 * edge and data sets.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 * @see Difference
 * @see Intersect
 * @see Union
 */
public abstract class SetOperator<VD extends VertexData, ED extends EdgeData,
  GD extends GraphData> extends
  AbstractBinaryCollectionToCollectionOperator<VD, ED, GD> {

  /**
   * Computes new vertices based on the new subgraphs. For each vertex, each
   * graph is collected in a flatMap function and then joined with the new
   * subgraph dataset.
   *
   * @param newSubgraphs graph dataset of the resulting graph collection
   * @return vertex set of the resulting graph collection
   */
  @Override
  protected DataSet<Vertex<Long, VD>> computeNewVertices(
    DataSet<Subgraph<Long, GD>> newSubgraphs) throws Exception {
    DataSet<Tuple2<Vertex<Long, VD>, Long>> verticesWithGraphs =
      firstGraph.getVertices().flatMap(
        new FlatMapFunction<Vertex<Long, VD>, Tuple2<Vertex<Long, VD>, Long>>
        () {
          @Override
          public void flatMap(Vertex<Long, VD> v,
            Collector<Tuple2<Vertex<Long, VD>, Long>> collector) throws
            Exception {
            for (Long graph : v.getValue().getGraphs()) {
              collector.collect(new Tuple2<>(v, graph));
            }
          }
        });

    return verticesWithGraphs
      .join(newSubgraphs)
      .where(1)
      .equalTo(new GraphKeySelector<GD>())
      .with(
        new JoinFunction<Tuple2<Vertex<Long, VD>, Long>, Subgraph<Long, GD>,
          Vertex<Long, VD>>() {
          @Override
          public Vertex<Long, VD> join(Tuple2<Vertex<Long, VD>, Long> vertices,
            Subgraph<Long, GD> subgraph) throws Exception {
            return vertices.f0;
          }
        })
      .distinct(new VertexKeySelector<VD>());
  }

  /**
   * Constructs new edges by joining the edges of the first graph with the new
   * vertices.
   *
   * @param newVertices vertex set of the resulting graph collection
   * @return edges set only connect vertices in {@code newVertices}
   * @see Difference
   * @see Intersect
   */
  @Override
  protected DataSet<Edge<Long, ED>> computeNewEdges(
    DataSet<Vertex<Long, VD>> newVertices) {
    return firstGraph.getEdges().join(newVertices)
      .where(new EdgeSourceVertexKeySelector<ED>())
      .equalTo(new VertexKeySelector<VD>())
      .with(new EdgeJoinFunction<VD, ED>()).join(newVertices)
      .where(new EdgeTargetVertexKeySelector<ED>())
      .equalTo(new VertexKeySelector<VD>())
      .with(new EdgeJoinFunction<VD, ED>())
      .distinct(new EdgeKeySelector<ED>());
  }
}
