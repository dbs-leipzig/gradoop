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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Initializes an EPGM edge from the given {@link ImportEdge}.
 *
 * @param <K> Import Edge/Vertex identifier type
 */
@FunctionAnnotation.ForwardedFieldsFirst(
  "f2->f0;" +           // import target vertex id
  "f3->f1.label;" +     // edge label
  "f4->f1.properties")  // edge properties
@FunctionAnnotation.ForwardedFieldsSecond(
  "f1->f1.sourceId"     // EPGM source vertex id
)
public class InitEdge<K extends Comparable<K>>
  extends InitElement<Edge, K>
  implements JoinFunction<ImportEdge<K>, Tuple2<K, GradoopId>, Tuple2<K, Edge>>,
  ResultTypeQueryable<Tuple2<K, Edge>> {

  /**
   * Used to create new EPGM edge.
   */
  private final EPGMEdgeFactory<Edge> edgeFactory;

  /**
   * Reduce object instantiation.
   */
  private final Tuple2<K, Edge> reuseTuple;

  /**
   * Creates a new join function.
   *
   * @param epgmEdgeFactory         edge factory
   * @param lineagePropertyKey  property key to store import identifier
   *                            (can be {@code null})
   * @param keyTypeInfo         type info for the import edge identifier
   */
  public InitEdge(EPGMEdgeFactory<Edge> epgmEdgeFactory, String lineagePropertyKey,
    TypeInformation<K> keyTypeInfo) {
    super(lineagePropertyKey, keyTypeInfo);
    this.edgeFactory        = epgmEdgeFactory;
    this.reuseTuple         = new Tuple2<>();
  }

  /**
   * Outputs a pair of import target vertex id and new EPGM edge. The target
   * vertex id is used for further joining the tuple with the import vertices.
   *
   * @param importEdge    import edge
   * @param vertexIdPair  pair of import id and corresponding Gradoop vertex id
   * @return pair of import target vertex id and EPGM edge
   * @throws Exception
   */
  @Override
  public Tuple2<K, Edge> join(ImportEdge<K> importEdge,
    Tuple2<K, GradoopId> vertexIdPair) throws Exception {
    reuseTuple.f0 = importEdge.getTargetId();

    Edge edge = edgeFactory.createEdge(importEdge.getLabel(),
      vertexIdPair.f1, GradoopId.get(), importEdge.getProperties());

    reuseTuple.f1 = updateLineage(edge, importEdge.getId());

    return reuseTuple;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<Tuple2<K, Edge>> getProducedType() {
    return new TupleTypeInfo<>(getKeyTypeInfo(),
      TypeExtractor.createTypeInfo(edgeFactory.getType()));
  }
}
