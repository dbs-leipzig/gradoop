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

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.functions.filters.CombinableFilter;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

/**
 * Filters graph transactions if their graph head identifier is not equal to the given identifier.
 */
@FunctionAnnotation.ReadFields("f0")
public class ByDifferentGraphId implements CombinableFilter<GraphTransaction> {
  /**
   * Graph head id
   */
  private final GradoopId graphId;

  /**
   * Constructor
   *
   * @param graphId graph identifier to filter
   */
  public ByDifferentGraphId(GradoopId graphId) {
    this.graphId = graphId;
  }

  @Override
  public boolean filter(GraphTransaction graphTransaction) throws Exception {
    return !graphTransaction.getGraphHead().getId().equals(graphId);
  }
}
