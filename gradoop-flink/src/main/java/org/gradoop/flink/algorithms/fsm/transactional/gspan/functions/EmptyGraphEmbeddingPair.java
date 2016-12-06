package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transactional.AdjacencyList;

import java.util.Map;

public class EmptyGraphEmbeddingPair implements MapFunction<Boolean, GraphEmbeddingsPair> {
  @Override
  public GraphEmbeddingsPair map(Boolean aBoolean) throws Exception {

    Map<GradoopId, String> labels = Maps.newHashMapWithExpectedSize(0);
    Map<GradoopId, Properties> properties = Maps.newHashMapWithExpectedSize(0);
    Map<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> rows =
      Maps.newHashMapWithExpectedSize(0);


    AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> adjacencyList =
      new AdjacencyList<>(new GraphHead(), labels, properties, rows, rows);

    return new GraphEmbeddingsPair(adjacencyList, Maps.newHashMap());
  }
}
