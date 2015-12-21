package org.gradoop.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.bool.Not;
import org.gradoop.model.impl.functions.bool.Or;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.join.OneSideEmpty;
import org.gradoop.model.impl.functions.tuple.ValueInTuple1;
import org.gradoop.model.impl.id.GradoopId;

public class CollectionEqualityByGraphIds
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements BinaryCollectionToValueOperator<G, V, E, Boolean> {

  @Override
  public DataSet<Boolean> execute(GraphCollection<G, V, E> firstCollection,
    GraphCollection<G, V, E> secondCollection) {

    DataSet<Tuple1<GradoopId>> distinctFirstGraphIds = firstCollection
      .getGraphHeads()
      .map(new Id<G>())
      .distinct()
      .map(new ValueInTuple1<GradoopId>());

    DataSet<Tuple1<GradoopId>> distinctSecondGraphIds = secondCollection
      .getGraphHeads()
      .map(new Id<G>())
      .distinct()
      .map(new ValueInTuple1<GradoopId>());

    DataSet<Boolean> d = distinctFirstGraphIds
      .fullOuterJoin(distinctSecondGraphIds)
      .where(0).equalTo(0)
      .with(new OneSideEmpty<Tuple1<GradoopId>, Tuple1<GradoopId>>())
      .union(firstCollection
        .getConfig()
        .getExecutionEnvironment()
        .fromElements(false)
      );

    return Not.map(Or.reduce(d));
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
