package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.tuple.ToIdWithLabel;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.representation.transactional.AdjacencyList;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.representation.transactional.RepresentationConverters;



public class ToAdjacencyList
  implements MapFunction<GraphTransaction, AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel>> {



  private MapFunction<Edge, IdWithLabel> edgeMapFunction = new ToIdWithLabel<>();
  private MapFunction<Vertex, IdWithLabel> vertexMapFunction = new ToIdWithLabel<>();

  @Override
  public AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> map(GraphTransaction graphTransaction) throws Exception {


    return RepresentationConverters.getAdjacencyList(
      graphTransaction, edgeMapFunction, vertexMapFunction);
  }
}
