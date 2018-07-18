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
package org.gradoop.flink.model.impl.operators.matching.common.debug;

import org.apache.log4j.Logger;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;

import java.util.Arrays;

/**
 * Debug output for {@link TripleWithCandidates}.
 *
 * @param <K> key type
 */
public class PrintTripleWithCandidates<K> extends Printer<TripleWithCandidates<K>, K> {
  /**
   * Logger
   */
  private static final Logger LOG = Logger.getLogger(PrintTripleWithCandidates.class);

  /**
   * Constructor
   */
  public PrintTripleWithCandidates() {
  }

  /**
   * Creates a new printer.
   *
   * @param isIterative true, if called in iterative context
   * @param prefix prefix for output
   */
  public PrintTripleWithCandidates(boolean isIterative, String prefix) {
    super(isIterative, prefix);
  }

  /**
   * Constructor
   *
   * @param iterationNumber true, if used in iterative context
   * @param prefix          prefix for debug string
   */
  public PrintTripleWithCandidates(int iterationNumber, String prefix) {
    super(iterationNumber, prefix);
  }

  @Override
  protected String getDebugString(TripleWithCandidates<K> t) {
    return String.format("(%s,%s,%s,%s)",
      edgeMap.get(t.getEdgeId()),
      vertexMap.get(t.getSourceId()),
      vertexMap.get(t.getTargetId()),
      Arrays.toString(t.getCandidates()));
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
