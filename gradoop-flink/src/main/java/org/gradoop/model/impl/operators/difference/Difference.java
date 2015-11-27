package org.gradoop.model.impl.operators.difference;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.operators.base.SetOperatorBase;

import java.util.Iterator;

/**
 * Returns a collection with all logical graphs that are contained in the
 * first input collection but not in the second.
 * Graph equality is based on their respective identifiers.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 * @see DifferenceBroadcast
 */
public class Difference<
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead>
  extends SetOperatorBase<VD, ED, GD> {

  /**
   * Computes the subgraph dataset for the resulting collection.
   *
   * @return subgraph dataset of the resulting collection
   */
  @Override
  protected DataSet<GD> computeNewGraphHeads() {
    // assign 1L to each subgraph in the first collection
    DataSet<Tuple2<GD, Long>> thisGraphs = firstCollection.getGraphHeads()
      .map(new Tuple2LongMapper<GD>(1L));
    // assign 2L to each subgraph in the second collection
    DataSet<Tuple2<GD, Long>> otherGraphs = secondCollection.getGraphHeads()
      .map(new Tuple2LongMapper<GD>(2L));

    // union the subgraphs, group them by their identifier and check that
    // there is no graph in the group that belongs to the second collection
    return thisGraphs.union(otherGraphs)
      .groupBy(new SubgraphTupleKeySelector<GD, Long>()).reduceGroup(
        new GroupReduceFunction<Tuple2<GD, Long>, GD>() {
          @Override
          public void reduce(
            Iterable<Tuple2<GD, Long>> iterable,
            Collector<GD> collector) throws Exception {
            Iterator<Tuple2<GD, Long>> it = iterable.iterator();
            Tuple2<GD, Long> graphHeadWithLong = null;
            boolean inOtherCollection = false;
            while (it.hasNext()) {
              graphHeadWithLong = it.next();
              if (graphHeadWithLong.f1.equals(2L)) {
                // graph head is in second collection
                inOtherCollection = true;
                break;
              }
            }
            if (!inOtherCollection && graphHeadWithLong != null) {
              collector.collect(graphHeadWithLong.f0);
            }
          }
        });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Difference.class.getName();
  }
}
