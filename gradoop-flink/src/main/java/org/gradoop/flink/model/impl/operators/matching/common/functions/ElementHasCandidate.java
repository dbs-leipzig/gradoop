/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.matching.common.tuples
  .IdWithCandidates;

/**
 * Filters elements if their candidates contain a given candidate.
 *
 * Read fields:
 *
 * f1: candidates
 */
@FunctionAnnotation.ReadFields("f1")
public class ElementHasCandidate implements FilterFunction<IdWithCandidates> {

  /**
   * Candidate to test on
   */
  private final int candidate;

  /**
   * Constructor
   *
   * @param candidate candidate to test on
   */
  public ElementHasCandidate(long candidate) {
    this.candidate = (int) candidate;
  }

  @Override
  public boolean filter(IdWithCandidates idWithCandidates) throws Exception {
    return idWithCandidates.getCandidates()[candidate];
  }
}
