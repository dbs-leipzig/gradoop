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
package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

/**
 * join functions to update the source vertex string representation of an
 * edge string representation
 */
public class SourceStringUpdater
  implements JoinFunction<EdgeString, VertexString, EdgeString> {

  @Override
  public EdgeString join(
    EdgeString edgeString, VertexString sourceLabel) throws Exception {

    edgeString.setSourceLabel(sourceLabel.getLabel());

    return edgeString;
  }
}
