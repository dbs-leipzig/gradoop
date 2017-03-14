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

package org.gradoop.flink.io.reader.parsers.rawedges.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Swaps the edges in order to create a unordered graph
 */
@FunctionAnnotation.ForwardedFields("f1 -> f2; f2 -> f1")
public class SwapImportedEdges implements MapFunction<ImportEdge<String>, ImportEdge<String>> {
  @Override
  public ImportEdge<String> map(ImportEdge<String> value) throws Exception {
    String tmp = value.getSourceId();
    value.setSourceId(value.getTargetId());
    value.setTargetId(tmp);
    value.setId(value.getSourceId() + "-" + value.getTargetId());
    return value;
  }
}
