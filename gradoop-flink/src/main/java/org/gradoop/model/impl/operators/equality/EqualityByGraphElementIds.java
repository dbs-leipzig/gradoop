package org.gradoop.model.impl.operators.equality;

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
import org.gradoop.model.impl.model.GraphCollection;
import org.gradoop.model.impl.functions.join.LeftSide;
import org.gradoop.model.impl.functions.counting.Tuple1With1L;
import org.gradoop.model.impl.functions.epgm.Tuple1WithId;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.operators.equality.functions
  .GraphIdElementIdInTuple2;
import org.gradoop.model.impl.operators.equality.functions
  .GraphIdElementIdsInTuple2;
import org.gradoop.model.impl.operators.equality.functions
  .GraphIdVertexIdsEdgeIdsTriple;
import org.gradoop.model.impl.operators.equality.functions
  .VertexIdsEdgeIdsCountTriple;


public class EqualityByGraphElementIds
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends EqualityBase
  implements BinaryCollectionToValueOperator<G, V, E, Boolean> {

  @Override
  public DataSet<Boolean> execute(GraphCollection<G, V, E> firstCollection,
    GraphCollection<G, V, E> secondCollection) {

    DataSet<Tuple3<GradoopIdSet, GradoopIdSet, Long>> firstGraphsWithCount =
      getGraphElementIdsWithCount(firstCollection);

    DataSet<Tuple3<GradoopIdSet, GradoopIdSet, Long>> secondGraphsWithCount =
      getGraphElementIdsWithCount(secondCollection);

    DataSet<Tuple1<Long>> distinctFirstGraphCount = firstGraphsWithCount
      .map(new Tuple1With1L<Tuple3<GradoopIdSet, GradoopIdSet, Long>>())
      .sum(0);

    DataSet<Tuple1<Long>> matchingIdCount = firstGraphsWithCount
      .join(secondGraphsWithCount)
      .where(0, 1, 2).equalTo(0, 1, 2)
      .with(new Tuple1With1L<Tuple3<GradoopIdSet, GradoopIdSet, Long>>())
      .sum(0);

    return checkCountEqualsCount(distinctFirstGraphCount, matchingIdCount);
  }



  private DataSet<Tuple3<GradoopIdSet, GradoopIdSet, Long>>
  getGraphElementIdsWithCount(GraphCollection<G, V, E> graphCollection) {

    DataSet<Tuple2<GradoopId, Long>> firstGraphIdOccurrences =
      getIdsWithCount(graphCollection);

    DataSet<Tuple1<GradoopId>> graphIds = graphCollection.getGraphHeads()
      .map(new Tuple1WithId<G>());

    DataSet<Tuple2<GradoopId,GradoopIdSet>> vertexIdsByGraphId =
      getElementIdsByGraphId(
        graphIds, graphCollection.getVertices());

    DataSet<Tuple2<GradoopId,GradoopIdSet>> edgeIdsByGraphId =
      getElementIdsByGraphId(
        graphIds, graphCollection.getEdges());

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
    <Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopIdSet>>
  getElementIdsByGraphId(
    DataSet<Tuple1<GradoopId>> graphIds, DataSet<T> elements) {
    return elements
      .flatMap(new GraphIdElementIdInTuple2<T>())
      .join(graphIds)
      .where(0).equalTo(0)
      .with(new LeftSide<Tuple2<GradoopId, GradoopId>,
            Tuple1<GradoopId>>())
      .groupBy(0)
      .reduceGroup(new GraphIdElementIdsInTuple2());
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
