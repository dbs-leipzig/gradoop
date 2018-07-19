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
package org.gradoop.flink.model.impl.functions.utils;

import org.apache.flink.api.common.functions.JoinFunction;

/**
 * Evaluates to true, if one join partner is NULL.
 * @param <L> left type
 * @param <R> right type
 */
public class OneSideEmpty<L, R> implements JoinFunction<L, R, Boolean> {

  @Override
  public Boolean join(L left, R right) throws Exception {
    return left == null || right == null;
  }
}
