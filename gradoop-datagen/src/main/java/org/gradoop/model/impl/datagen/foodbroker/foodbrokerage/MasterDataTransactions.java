package org.gradoop.model.impl.datagen.foodbroker.foodbrokerage;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.config.GradoopConfig;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Stephan on 04.08.16.
 */
public class MasterDataTransactions<G extends EPGMGraphHead,
  V extends EPGMVertex, E extends EPGMEdge>
  implements GroupReduceFunction<V, GraphTransaction<G, V, E>> {


  private GradoopFlinkConfig<G, V, E> config;

  public MasterDataTransactions(GradoopFlinkConfig<G, V, E> config) {
    this.config = config;
  }

  @Override
  public void reduce(Iterable<V> iterable,
    Collector<GraphTransaction<G, V, E>> collector) throws Exception {
    GraphTransaction<G, V, E> graphTransaction = new GraphTransaction
      <G, V, E>();
    Set<V> set = Sets.newHashSet();
    for (V v : iterable) {
      set.add(v);
    }
    Set<E> edge = Sets.newHashSet();
    edge.add(config.getEdgeFactory().createEdge(set.iterator().next().getId()
      , set.iterator().next().getId()));
    graphTransaction.setVertices(set);
    graphTransaction.setGraphHead(config
      .getGraphHeadFactory().createGraphHead());
    graphTransaction.setEdges(edge);
//    graphTransaction.setEdges(Sets.<E>newHashSet());
    collector.collect(graphTransaction);
  }
}
