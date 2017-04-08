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

package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.flink.io.impl.graph.functions.InitElement;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Extends the definition of InitVertex for associating each vertex to a specific graph by value
 * @param <K> vertex-id parameter
 */
public class InitEdgeForCollection<K extends Comparable<K>>
  extends InitElement<Edge, K>
  implements JoinFunction<ImportEdge<K>, Tuple2<K, GradoopId>, Tuple3<K, K, Edge>>,
  ResultTypeQueryable<Tuple3<K, K, Edge>> {


  /**
   * Used to create new EPGM edge.
   */
  private final EdgeFactory edgeFactory;

  /**
   * Reduce object instantiation.
   */
  private final Tuple3<K, K, Edge> reuseTuple;

  /**
   * Creates a new join function.
   *
   * @param edgeFactory         edge factory
   * @param lineagePropertyKey  property key to store import identifier
   *                            (can be {@code null})
   * @param keyTypeInfo         type info for the import edge identifier
   */
  public InitEdgeForCollection(EdgeFactory edgeFactory, String lineagePropertyKey,
    TypeInformation<K> keyTypeInfo) {
    super(lineagePropertyKey, keyTypeInfo);
    this.edgeFactory        = edgeFactory;
    this.reuseTuple         = new Tuple3<>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<Tuple3<K, K, Edge>> getProducedType() {
    return new TupleTypeInfo<>(getKeyTypeInfo(), getKeyTypeInfo(),
      TypeExtractor.createTypeInfo(edgeFactory.getType()));
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
  public Tuple3<K, K, Edge> join(ImportEdge<K> importEdge,
    Tuple2<K, GradoopId> vertexIdPair) throws Exception {
    reuseTuple.f0 = importEdge.getTargetId();

    Edge edge = edgeFactory.createEdge(importEdge.getLabel(),
      vertexIdPair.f1, GradoopId.get(), importEdge.getProperties());

    reuseTuple.f1 = importEdge.getId();
    reuseTuple.f2 = updateLineage(edge, importEdge.getId());

    return reuseTuple;
  }
}
