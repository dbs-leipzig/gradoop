package org.gradoop.model.impl.operators.collection.binary.equality;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.counting.OneInTuple1;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIds;
import org.gradoop.model.impl.operators.collection.binary.equality.functions
  .EqualityHelper;
import org.gradoop.model.impl.operators.collection.binary.equality.functions
  .GraphIdElementIdInTuple2;
import org.gradoop.model.impl.operators.collection.binary.equality.functions
  .GraphIdElementIdsInTuple2;
import org.gradoop.model.impl.operators.collection.binary.equality.functions
  .GraphIdVertexIdsEdgeIdsTriple;
import org.gradoop.model.impl.operators.collection.binary.equality.functions
  .VertexIdsEdgeIdsCountTriple;


public class EqualByGraphElementIds
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements BinaryCollectionToValueOperator<G, V, E, Boolean> {

  @Override
  public DataSet<Boolean> execute(GraphCollection<V, E, G> firstCollection,
    GraphCollection<V, E, G> secondCollection) {

    DataSet<Tuple3<GradoopIds, GradoopIds, Long>> firstGraphsWithCount =
      getGraphElementIdsWithCount(firstCollection);

    DataSet<Tuple3<GradoopIds, GradoopIds, Long>> secondGraphsWithCount =
      getGraphElementIdsWithCount(firstCollection);

    DataSet<Tuple1<Long>> distinctFirstGraphCount = firstGraphsWithCount
      .map(new OneInTuple1<Tuple3<GradoopIds, GradoopIds, Long>>())
      .sum(0);

    DataSet<Tuple1<Long>> matchingIdCount = firstGraphsWithCount
      .join(secondGraphsWithCount)
      .where(0, 1, 2).equalTo(0, 1, 2)
      .with(new OneInTuple1<Tuple3<GradoopIds, GradoopIds, Long>>())
      .sum(0);

    return EqualityHelper.checkCountEqualsCount(
      distinctFirstGraphCount, matchingIdCount);
  }

  private DataSet<Tuple3<GradoopIds, GradoopIds, Long>>
  getGraphElementIdsWithCount(GraphCollection<V, E, G> graphCollection) {

    DataSet<Tuple2<GradoopId, Long>> firstGraphIdOccurrences =
      EqualityHelper.getIdsWithCount(graphCollection);



    DataSet<Tuple2<GradoopId,GradoopIds>> vertexIdsByGraphId =
      getElementIdsByGraphId(graphCollection.getVertices());

    DataSet<Tuple2<GradoopId,GradoopIds>> edgeIdsByGraphId =
      getElementIdsByGraphId(graphCollection.getEdges());

    return vertexIdsByGraphId
      .join(edgeIdsByGraphId)
      .where(0).equalTo(0)
      .with(new GraphIdVertexIdsEdgeIdsTriple())
      .join(firstGraphIdOccurrences)
      .where(0).equalTo(0)
      .with(new VertexIdsEdgeIdsCountTriple())
      .groupBy(0, 1)
      .sum(2);
  }

  private <T extends EPGMGraphElement> GroupReduceOperator
    <Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopIds>>
  getElementIdsByGraphId(DataSet<T> elements) {
    return elements
      .flatMap(new GraphIdElementIdInTuple2<T>())
      .groupBy(0)
      .reduceGroup(new GraphIdElementIdsInTuple2());
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
