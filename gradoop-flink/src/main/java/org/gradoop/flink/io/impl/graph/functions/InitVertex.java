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
package org.gradoop.flink.io.impl.graph.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Initializes an EPGM vertex from the given {@link ImportVertex}.
 *
 * @param <K> Import Edge/Vertex identifier type
 */
@FunctionAnnotation.ForwardedFields(
  "f0;" + // vertex id
  "f1->f2.label;" + // vertex label
  "f2->f2.properties" // vertex properties
)
public class InitVertex<K extends Comparable<K>>
  extends InitElement<Vertex, K>
  implements MapFunction<ImportVertex<K>, Tuple3<K, GradoopId, Vertex>>,
  ResultTypeQueryable<Tuple3<K, GradoopId, Vertex>> {

  /**
   * Used to create new EPGM vertex.
   */
  private final EPGMVertexFactory<Vertex> vertexFactory;

  /**
   * Reduce object instantiation.
   */
  private final Tuple3<K, GradoopId, Vertex> reuseTuple;

  /**
   * Creates a new map function
   * @param epgmVertexFactory       vertex factory
   * @param lineagePropertyKey  property key to store import identifier
   *                            (can be {@code null})
   * @param externalIdType         type info for the import vertex identifier
   */
  public InitVertex(EPGMVertexFactory<Vertex> epgmVertexFactory,
    String lineagePropertyKey, TypeInformation<K> externalIdType) {
    super(lineagePropertyKey, externalIdType);
    this.vertexFactory      = epgmVertexFactory;
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
  public Tuple3<K, GradoopId, Vertex> map(ImportVertex<K> importVertex) throws
    Exception {
    reuseTuple.f0 = importVertex.getId();

    Vertex vertex = vertexFactory.createVertex(importVertex.getLabel(),
      importVertex.getProperties());

    reuseTuple.f1 = vertex.getId();
    reuseTuple.f2 = updateLineage(vertex, importVertex.getId());

    return reuseTuple;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<Tuple3<K, GradoopId, Vertex>> getProducedType() {
    return new TupleTypeInfo<>(getKeyTypeInfo(),
      TypeExtractor.getForClass(GradoopId.class),
      TypeExtractor.createTypeInfo(vertexFactory.getType()));
  }
}
