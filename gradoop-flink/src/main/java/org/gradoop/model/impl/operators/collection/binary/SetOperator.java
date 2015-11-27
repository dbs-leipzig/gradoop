package org.gradoop.model.impl.operators.collection.binary;

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

/**
 * Base class for set operations that share common methods to build vertex,
 * edge and data sets.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 * @see Difference
 * @see Intersect
 * @see Union
 */
public abstract class SetOperator<
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead>
  extends BinaryCollectionToCollectionOperatorBase<VD, ED, GD> {

  /**
   * Computes new vertices based on the new subgraphs. For each vertex, each
   * graph is collected in a flatMap function and then joined with the new
   * subgraph dataset.
   *
   * @param newGraphHeads graph dataset of the resulting graph collection
   * @return vertex set of the resulting graph collection
   */
  @Override
  protected DataSet<VD> computeNewVertices(
    DataSet<GD> newGraphHeads) throws Exception {
    DataSet<Tuple2<VD, GradoopId>> verticesWithGraphs =
      firstCollection.getVertices().flatMap(
        new FlatMapFunction<VD, Tuple2<VD, GradoopId>>() {
          @Override
          public void flatMap(VD v,
            Collector<Tuple2<VD, GradoopId>> collector) throws
            Exception {
            for (GradoopId graphId : v.getGraphIds()) {
              collector.collect(new Tuple2<>(v, graphId));
            }
          }
        });

    return verticesWithGraphs
      .join(newGraphHeads)
      .where(1)
      .equalTo(new Id<GD>())
      .with(
        new JoinFunction<Tuple2<VD, GradoopId>, GD, VD>() {
          @Override
          public VD join(Tuple2<VD, GradoopId> vertices,
            GD subgraph) throws Exception {
            return vertices.f0;
          }
        })
      .distinct(new Id<VD>());
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
  protected DataSet<ED> computeNewEdges(DataSet<VD> newVertices) {
    return firstCollection.getEdges().join(newVertices)
      .where(new SourceId<ED>())
      .equalTo(new Id<VD>())
      .with(new LeftSide<ED, VD>())
      .join(newVertices)
      .where(new TargetId<ED>())
      .equalTo(new Id<VD>())
      .with(new LeftSide<ED, VD>())
      .distinct(new Id<ED>());
  }
}
