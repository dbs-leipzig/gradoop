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

package org.gradoop.io.graph.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.io.graph.tuples.ImportVertex;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Initializes an EPGM vertex from the given {@link ImportVertex}.
 *
 * @param <V> EPGM vertex type
 * @param <K> Import Edge/Vertex identifier type
 */
public class InitEPGMVertex<V extends EPGMVertex, K extends Comparable<K>>
  extends InitEPGMElement<V, K>
  implements MapFunction<ImportVertex<K>, Tuple3<K, GradoopId, V>>,
  ResultTypeQueryable<Tuple3<K, GradoopId, V>> {

  /**
   * Used to create new EPGM vertex.
   */
  private final EPGMVertexFactory<V> vertexFactory;

  /**
   * Reduce object instantiation.
   */
  private final Tuple3<K, GradoopId, V> reuseTuple;

  /**
   * Creates a new map function
   * @param vertexFactory       vertex factory
   * @param lineagePropertyKey  property key to store import identifier
   *                            (can be {@code null})
   * @param keyTypeInfo         type info for the import vertex identifier
   */
  public InitEPGMVertex(EPGMVertexFactory<V> vertexFactory,
    String lineagePropertyKey, TypeInformation<K> keyTypeInfo) {
    super(lineagePropertyKey, keyTypeInfo);
    this.vertexFactory      = vertexFactory;
    this.reuseTuple         = new Tuple3<>();
  }

  /**
   * Outputs a triple containing of the import vertex identifier, the new EPGM
   * vertex identifier and the EPGM vertex.
   *
   * @param importVertex import vertex
   * @return triple containing import and EPGM id as well as the EPGM vertex
   * @throws Exception
   */
  @Override
  public Tuple3<K, GradoopId, V> map(ImportVertex<K> importVertex) throws
    Exception {
    reuseTuple.f0 = importVertex.getId();

    V vertex = vertexFactory.createVertex(importVertex.getLabel(),
      importVertex.getProperties());

    reuseTuple.f1 = vertex.getId();
    reuseTuple.f2 = updateLineage(vertex, importVertex.getId());

    return reuseTuple;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<Tuple3<K, GradoopId, V>> getProducedType() {
    return new TupleTypeInfo<>(getKeyTypeInfo(),
      TypeExtractor.getForClass(GradoopId.class),
      TypeExtractor.createTypeInfo(vertexFactory.getType()));
  }
}
