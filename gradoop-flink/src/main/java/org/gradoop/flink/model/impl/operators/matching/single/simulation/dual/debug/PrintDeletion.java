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
package org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.debug;

import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.common.debug.Printer;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples
  .Deletion;

/**
 * Debug output for {@link Deletion}.
 */
public class PrintDeletion extends Printer<Deletion, GradoopId> {

  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintDeletion.class);

  /**
   * Constructor
   *
   * @param isIterative true, if called in iterative context
   * @param prefix prefix for output
   */
  public PrintDeletion(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  @Override
  protected String getDebugString(Deletion d) {
    return String.format("(%s,%s,%s,%s)",
      vertexMap.get(d.getRecipientId()),
      vertexMap.get(d.getSenderId()),
      d.getDeletion(),
      d.getMessageType());
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
