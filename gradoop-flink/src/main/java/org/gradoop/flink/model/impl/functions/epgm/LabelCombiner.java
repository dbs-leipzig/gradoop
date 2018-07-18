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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.api.entities.EPGMLabeled;

/**
 * concatenates the labels of labeled things
 * @param <L> labeled type
 */
public class LabelCombiner<L extends EPGMLabeled> implements
  JoinFunction<L, L, L> {

  @Override
  public L join(L left, L right) throws Exception {

    String rightLabel = right == null ? "" : right.getLabel();

    left.setLabel(left.getLabel() + rightLabel);

    return left;
  }
}
