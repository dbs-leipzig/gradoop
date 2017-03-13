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

package org.gradoop.flink.io.reader.parsers.rawedges;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Extract the source and the target information from the single edge
 */
public class CollectSourceAndDest implements
  FlatMapFunction<ImportEdge<String>, ImportVertex<String>> {

  /**
   * Reusable element acting both as a source and as a target
   */
  private final ImportVertex<String> reusable;

  /**
   * Informations concerning
   */
  private final Properties reusableProperties;

  /**
   * Default constructor
   * @param label Specifiying the vertex label
   */
  public CollectSourceAndDest(String label) {
    this.reusable = new ImportVertex<>();
    this.reusable.setLabel(label);
    reusableProperties = new Properties();
  }

  /**
   * Default constructor
   */
  public CollectSourceAndDest() {
    this("Vertex");
  }

  @Override
  public void flatMap(ImportEdge<String> value, Collector<ImportVertex<String>> out) throws
    Exception {
    reusableProperties.set("id", value.getSourceId());
    reusable.setId(value.getSourceId());
    out.collect(reusable);
    reusableProperties.set("id", value.getTargetId());
    reusable.setId(value.getTargetId());
    out.collect(reusable);
  }

}
