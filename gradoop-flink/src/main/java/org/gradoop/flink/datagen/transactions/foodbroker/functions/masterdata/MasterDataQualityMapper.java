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
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerPropertyKeys;

/**
 * Creates a master data tuple from the given vertex. The tuple consists of the gradoop id and
 * the quality.
 */
public class MasterDataQualityMapper implements MapFunction<Vertex, Tuple2<GradoopId, Float>> {

  @Override
  public Tuple2<GradoopId, Float> map(Vertex v) throws Exception {
    return new Tuple2<>(v.getId(), v.getPropertyValue(
      FoodBrokerPropertyKeys.QUALITY_KEY).getFloat());
  }
}
