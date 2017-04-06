package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.flink.io.impl.graph.functions.InitElement;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Extends the definition of InitVertex for associating each vertex to a specific graph by value
 * @param <K> vertex-id parameter
 */
public class InitEdgeForCollection2<K extends Comparable<K>>
  extends InitElement<Edge, K>
  implements JoinFunction<Tuple2<ImportEdge<K>,GradoopIdList>, Tuple2<K, GradoopId>, Tuple2<K, Edge>>,
  ResultTypeQueryable<Tuple2<K, Edge>> {


  /**
   * Used to create new EPGM edge.
   */
  private final EdgeFactory edgeFactory;

  /**
   * Reduce object instantiation.
   */
  private final Tuple2<K, Edge> reuseTuple;

  /**
   * Creates a new join function.
   *
   * @param edgeFactory         edge factory
   * @param lineagePropertyKey  property key to store import identifier
   *                            (can be {@code null})
   * @param keyTypeInfo         type info for the import edge identifier
   */
  public InitEdgeForCollection2(EdgeFactory edgeFactory, String lineagePropertyKey,
    TypeInformation<K> keyTypeInfo) {
    super(lineagePropertyKey, keyTypeInfo);
    this.edgeFactory        = edgeFactory;
    this.reuseTuple         = new Tuple2<>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<Tuple2<K, Edge>> getProducedType() {
    return new TupleTypeInfo<>(getKeyTypeInfo(),
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
  public Tuple2<K, Edge> join(Tuple2<ImportEdge<K>, GradoopIdList> importEdge,
    Tuple2<K, GradoopId> vertexIdPair) throws Exception {
    reuseTuple.f0 = importEdge.f0.getTargetId();

    Edge edge = edgeFactory.createEdge(importEdge.f0.getLabel(),
      vertexIdPair.f1, GradoopId.get(), importEdge.f0.getProperties());
    edge.setGraphIds(importEdge.f1);

    reuseTuple.f1 = updateLineage(edge, importEdge.f0.getId());

    return reuseTuple;
  }
}
