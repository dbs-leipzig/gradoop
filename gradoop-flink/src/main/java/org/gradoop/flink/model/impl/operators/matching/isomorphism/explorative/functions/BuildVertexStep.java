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

package org.gradoop.flink.model.impl.operators.matching.isomorphism.explorative.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.model.impl.operators.matching.common.tuples
  .IdWithCandidates;
import org.gradoop.flink.model.impl.operators.matching.isomorphism
  .explorative.tuples.VertexStep;

/**
 * (id, [candidates]) -> (id)
 *
 * Forwarded fields:
 *
 * f0: vertex id
 *
 * Read fields:
 *
 * f0: vertex id
 */
@FunctionAnnotation.ForwardedFields("f0")
@FunctionAnnotation.ReadFields("f0")
public class BuildVertexStep implements MapFunction<IdWithCandidates, VertexStep> {
  /**
   * Reduce instantiations
   */
  private final VertexStep reuseTuple;

  /**
   * Constructor
   */
  public BuildVertexStep() {
    reuseTuple = new VertexStep();
  }

  @Override
  public VertexStep map(IdWithCandidates v) throws Exception {
    reuseTuple.setVertexId(v.getId());
    return reuseTuple;
  }
}
