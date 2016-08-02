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

package org.gradoop.flink.io.impl.graph.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Initializes an EPGM vertex from the given {@link ImportVertex}.
 *
 * @param <K> Import Edge/EPGMVertex identifier type
 */
@FunctionAnnotation.ForwardedFields(
  "f0;" + // vertex id
  "f1->f2.label;" + // vertex label
  "f2->f2.properties" // vertex properties
)
public class InitEPGMVertex<K extends Comparable<K>>
  extends InitEPGMElement<EPGMVertex, K>
  implements MapFunction<ImportVertex<K>, Tuple3<K, GradoopId, EPGMVertex>>,
  ResultTypeQueryable<Tuple3<K, GradoopId, EPGMVertex>> {

  /**
   * Used to create new EPGM vertex.
   */
  private final EPGMVertexFactory vertexFactory;

  /**
   * Reduce object instantiation.
   */
  private final Tuple3<K, GradoopId, EPGMVertex> reuseTuple;

  /**
   * Creates a new map function
   * @param vertexFactory       vertex factory
   * @param lineagePropertyKey  property key to store import identifier
   *                            (can be {@code null})
   * @param keyTypeInfo         type info for the import vertex identifier
   */
  public InitEPGMVertex(EPGMVertexFactory vertexFactory,
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
  public Tuple3<K, GradoopId, EPGMVertex> map(ImportVertex<K> importVertex) throws
    Exception {
    reuseTuple.f0 = importVertex.getId();

    EPGMVertex vertex = vertexFactory.createVertex(importVertex.getLabel(),
      importVertex.getProperties());

    reuseTuple.f1 = vertex.getId();
    reuseTuple.f2 = updateLineage(vertex, importVertex.getId());

    return reuseTuple;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<Tuple3<K, GradoopId, EPGMVertex>> getProducedType() {
    return new TupleTypeInfo<>(getKeyTypeInfo(),
      TypeExtractor.getForClass(GradoopId.class),
      TypeExtractor.createTypeInfo(vertexFactory.getType()));
  }
}
