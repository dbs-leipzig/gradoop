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
package org.gradoop.flink.model.api.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;

import java.io.Serializable;

/**
 * Marker interface for reduce functions used for group by isomorphism operator.
 * Such functions can be used to calculate aggregates based on graph heads.
 * For example, to count isomorphic graphs in a collection.
 */
public interface GraphHeadReduceFunction
  extends GroupReduceFunction<Tuple2<String, GraphHead>, GraphHead>, Serializable {
}
