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

package org.gradoop.io.impl.tsv.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.io.impl.graph.tuples.ImportVertex;
import org.gradoop.model.impl.properties.PropertyList;

/**
 * Class to create ImportVertices
 */
public class TupleToImportVertex
  implements FlatMapFunction<Tuple4<Long, String, Long, String>,
  ImportVertex<Long>> {

  /**
   * reused ImportVertex
   */
  private ImportVertex<Long> reuseVertex;

  /**
   * Label of the vertex
   */
  private String vertexLabel;

  /**
   * PropertyKey of property value
   */
  private String propertyKey;

  /**
   * Constructor
   *
   * @param vertexLabel used vertex label
   * @param propertyKey used PropertyKey
   */
  public TupleToImportVertex(String vertexLabel, String propertyKey) {
    this.vertexLabel = vertexLabel;
    this.propertyKey = propertyKey;
    this.reuseVertex = new ImportVertex<>();
  }

  /**
   * Method to collect ImportVertices
   *
   * @param lineTuple information of one line of the tsv input
   * @param collector collector
   * @throws Exception
   */
  @Override
  public void flatMap(Tuple4<Long, String, Long, String> lineTuple,
    Collector<ImportVertex<Long>> collector) throws Exception {
    collector.collect(initiateVertex(lineTuple.f0, lineTuple.f1));
    collector.collect(initiateVertex(lineTuple.f2, lineTuple.f3));
  }

  /**
   * Method to initialize the ImportVertex
   *
   * @param id      vertexId
   * @param value   value of the property
   * @return        initialized ImportVertex
   */
  private ImportVertex<Long> initiateVertex(long id, String value) {
    reuseVertex.setId(id);
    reuseVertex.setLabel("");
    PropertyList properties = new PropertyList();
    properties.set(propertyKey, value);
    reuseVertex.setProperties(properties);
    return reuseVertex;
  }
}
