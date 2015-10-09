package org.gradoop.model.impl.operators.collection;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.tuples.Subgraph;

import java.util.Iterator;

/**
 * Returns a collection with all logical graphs that are contained in the
 * first input collection but not in the second.
 * Graph equality is based on their respective identifiers.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 * @see DifferenceUsingList
 */
public class Difference<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData>
  extends SetOperator<VD, ED, GD> {

  /**
   * Computes the subgraph dataset for the resulting collection.
   *
   * @return subgraph dataset of the resulting collection
   */
  @Override
  protected DataSet<Subgraph<Long, GD>> computeNewSubgraphs() {
    // assign 1L to each subgraph in the first collection
    DataSet<Tuple2<Subgraph<Long, GD>, Long>> thisGraphs =
      firstSubgraphs.map(new Tuple2LongMapper<Subgraph<Long, GD>>(1L));
    // assign 2L to each subgraph in the second collection
    DataSet<Tuple2<Subgraph<Long, GD>, Long>> otherGraphs =
      secondSubgraphs.map(new Tuple2LongMapper<Subgraph<Long, GD>>(2L));

    // union the subgraphs, group them by their identifier and check that
    // there is no graph in the group that belongs to the second collection
    return thisGraphs.union(otherGraphs)
      .groupBy(new SubgraphTupleKeySelector<GD, Long>()).reduceGroup(
        new GroupReduceFunction<Tuple2<Subgraph<Long, GD>, Long>,
          Subgraph<Long, GD>>() {

          @Override
          public void reduce(
            Iterable<Tuple2<Subgraph<Long, GD>, Long>> iterable,
            Collector<Subgraph<Long, GD>> collector) throws Exception {
            Iterator<Tuple2<Subgraph<Long, GD>, Long>> it = iterable.iterator();
            Tuple2<Subgraph<Long, GD>, Long> subgraph = null;
            boolean inOtherCollection = false;
            while (it.hasNext()) {
              subgraph = it.next();
              if (subgraph.f1.equals(2L)) { // subgraph is in second collection
                inOtherCollection = true;
                break;
              }
            }
            if (!inOtherCollection && subgraph != null) {
              collector.collect(subgraph.f0);
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
