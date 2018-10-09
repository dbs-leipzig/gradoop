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
package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * combines string representations of multiple (parallel) edges
 */
public class MultiEdgeStringCombiner implements
  GroupReduceFunction<EdgeString, EdgeString> {

  @Override
  public void reduce(Iterable<EdgeString> iterable,
    Collector<EdgeString> collector) throws Exception {

    List<String> labels = new ArrayList<>();
    boolean first = true;
    EdgeString combinedLabel = null;

    for (EdgeString edgeString : iterable) {
      if (first) {
        combinedLabel = edgeString;
        first = false;
      }

      labels.add(edgeString.getEdgeLabel());
    }

    Collections.sort(labels);

    if (combinedLabel != null) {
      combinedLabel.setEdgeLabel(StringUtils.join(labels, "&"));
    }

    collector.collect(combinedLabel);
  }
}
