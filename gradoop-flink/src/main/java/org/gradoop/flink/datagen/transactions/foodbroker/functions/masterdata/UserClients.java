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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Set;

/**
 * Collects all users and clients given by sets into one dataset.
 */
public class UserClients implements FlatMapFunction<Set<Vertex>, Vertex> {

  @Override
  public void flatMap(Set<Vertex> vertices,
    Collector<Vertex> collector) throws Exception {
    for (Vertex vertex : vertices) {
      collector.collect(vertex);
    }
  }
}
