/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.flink.model.impl.functions.utils.OneSideEmpty;

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
      .map(new ObjectTo1<>());

    DataSet<Tuple1<GradoopId>> distinctSecondGraphIds = secondCollection
      .getGraphHeads()
      .map(new Id<GraphHead>())
      .distinct()
      .map(new ObjectTo1<>());

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
