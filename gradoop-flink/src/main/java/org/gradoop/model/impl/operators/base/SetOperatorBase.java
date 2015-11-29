package org.gradoop.model.impl.operators.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.join.LeftSide;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.functions.epgm.TargetId;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.difference.Difference;
import org.gradoop.model.impl.operators.intersection.Intersection;
import org.gradoop.model.impl.operators.union.Union;

/**
 * Base class for set operations that share common methods to build vertex,
 * edge and data sets.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @see Difference
 * @see Intersection
 * @see Union
 */
public abstract class SetOperatorBase<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  extends BinaryCollectionToCollectionOperatorBase<G, V, E> {

  /**
   * Computes new vertices based on the new subgraphs. For each vertex, each
   * graph is collected in a flatMap function and then joined with the new
   * subgraph dataset.
   *
   * @param newGraphHeads graph dataset of the resulting graph collection
   * @return vertex set of the resulting graph collection
   */
  @Override
  protected DataSet<V> computeNewVertices(
    DataSet<G> newGraphHeads) throws Exception {
    DataSet<Tuple2<V, GradoopId>> verticesWithGraphs =
      firstCollection.getVertices().flatMap(
        new FlatMapFunction<V, Tuple2<V, GradoopId>>() {
          @Override
          public void flatMap(V v,
            Collector<Tuple2<V, GradoopId>> collector) throws
            Exception {
            for (GradoopId graphId : v.getGraphIds()) {
              collector.collect(new Tuple2<>(v, graphId));
            }
          }
        });

    return verticesWithGraphs
      .join(newGraphHeads)
      .where(1)
      .equalTo(new Id<G>())
      .with(
        new JoinFunction<Tuple2<V, GradoopId>, G, V>() {
          @Override
          public V join(Tuple2<V, GradoopId> vertices,
            G subgraph) throws Exception {
            return vertices.f0;
          }
        })
      .distinct(new Id<V>());
  }

  /**
   * Constructs new edges by joining the edges of the first graph with the new
   * vertices.
   *
   * @param newVertices vertex set of the resulting graph collection
   * @return edges set only connect vertices in {@code newVertices}
   * @see Difference
   * @see Intersection
   */
  @Override
  protected DataSet<E> computeNewEdges(DataSet<V> newVertices) {
    return firstCollection.getEdges().join(newVertices)
      .where(new SourceId<E>())
      .equalTo(new Id<V>())
      .with(new LeftSide<E, V>())
      .join(newVertices)
      .where(new TargetId<E>())
      .equalTo(new Id<V>())
      .with(new LeftSide<E, V>())
      .distinct(new Id<E>());
  }
}
