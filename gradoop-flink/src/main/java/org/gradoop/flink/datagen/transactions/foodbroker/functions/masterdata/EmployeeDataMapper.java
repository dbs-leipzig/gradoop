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
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.EmployeeData;

/**
 * Creates a tuple from the given vertex. The tuple consists of the gradoop id and the relevant
 * person data.
 */
public class EmployeeDataMapper implements MapFunction<Vertex, Tuple2<GradoopId, EmployeeData>> {

  /**
   * Reduce object instantiation.
   */
  private EmployeeData reuseEmployeeData;

  /**
   * Constructor for object instantiation.
   */
  public EmployeeDataMapper() {
    reuseEmployeeData = new EmployeeData();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Tuple2<GradoopId, EmployeeData> map(Vertex v) throws Exception {
    reuseEmployeeData.setQuality(v.getPropertyValue(FoodBrokerPropertyKeys.QUALITY_KEY).getFloat());
    reuseEmployeeData.setCity(v.getPropertyValue(FoodBrokerPropertyKeys.CITY_KEY).getString());
    return new Tuple2<>(v.getId(), reuseEmployeeData);
  }
}
