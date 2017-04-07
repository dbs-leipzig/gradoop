package org.gradoop.benchmark.nesting.functions;

import com.sun.xml.bind.v2.schemagen.xmlschema.Import;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.impl.graph.functions.InitEdge;
import org.gradoop.flink.io.impl.graph.functions.InitElement;
import org.gradoop.flink.io.impl.graph.functions.InitVertex;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

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
