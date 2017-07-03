package org.gradoop.flink.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.functions.tuple.ValueInTuple1;
import org.gradoop.flink.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.utils.OneSideEmpty;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Operator to determine if two collections contain the same graphs by id.
 */
public class CollectionEqualityByGraphIds
  implements BinaryCollectionToValueOperator<Boolean> {

  @Override
  public DataSet<Boolean> execute(GraphCollection firstCollection,
    GraphCollection secondCollection) {

    DataSet<Tuple1<GradoopId>> distinctFirstGraphIds = firstCollection
      .getGraphHeads()
      .map(new Id<GraphHead>())
      .distinct()
      .map(new ValueInTuple1<GradoopId>());

    DataSet<Tuple1<GradoopId>> distinctSecondGraphIds = secondCollection
      .getGraphHeads()
      .map(new Id<GraphHead>())
      .distinct()
      .map(new ValueInTuple1<GradoopId>());

    DataSet<Boolean> d = distinctFirstGraphIds
      .fullOuterJoin(distinctSecondGraphIds)
      .where(0).equalTo(0)
      .with(new OneSideEmpty<Tuple1<GradoopId>, Tuple1<GradoopId>>())
      .union(firstCollection.getConfig()
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
