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
package org.gradoop.flink.model.impl.operators.cloning.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Replaces the graph set of each element by a new one, containing only the
 * first element of a broadcast GradoopId DataSet.
 *
 * @param <EL> EPGM element type
 */
@FunctionAnnotation.ForwardedFields("id;label;properties")
public class ElementGraphUpdater<EL extends GraphElement>
  extends RichMapFunction<EL, EL> {

  /**
   * constant string for accessing broadcast variable "graph id"
   */
  public static final String GRAPHID = "graph id";

  /**
   * id of the new graph
   */
  private GradoopId graphId;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.graphId = (GradoopId) getRuntimeContext()
      .getBroadcastVariable(GRAPHID).get(0);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public EL map(EL element) {
    element.setGraphIds(GradoopIdSet.fromExisting(graphId));
    return element;
  }
}
