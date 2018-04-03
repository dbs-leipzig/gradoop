package org.gradoop.flink.algorithms.jaccardindex.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.Denominator;

import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.DEFAULT_JACCARD_EDGE_PROPERTY;

/**
 * COPIED WITH SMALL MODIFICATIONS FROM
 * {@link org.apache.flink.graph.library.similarity.JaccardIndex}
 * Compute the counts of shared and distinct neighbors. A two-path connecting
 * the vertices is emitted for each shared neighbor. The number of distinct
 * neighbors is equal to the sum of degrees of the vertices minus the count
 * of shared numbers, which are double-counted in the degree sum.
 */
//@FunctionAnnotation.ForwardedFields("0; 1")
public class ComputeScores implements
  GroupReduceFunction<Tuple3<GradoopId, GradoopId, IntValue>, Edge> {

  private String edgeLabel;
  private Denominator denominator;

  public ComputeScores(String edgeLabel, Denominator denominator) {
    this.edgeLabel = edgeLabel;
    this.denominator = denominator;
  }

  @Override
  public void reduce(Iterable<Tuple3<GradoopId, GradoopId, IntValue>> values,
    Collector<Edge> out) throws Exception {
    int count = 0;
    Tuple3<GradoopId, GradoopId, IntValue> edge = null;


    for (Tuple3<GradoopId, GradoopId, IntValue> next : values) {
      edge = next;
      count += 1;
    }

    // TODO hier int overflow abfangen?
    // TODO wenn in GenerateGroupPairs max statt sum gewählt wurde muss hier nichts mehr
    // abgezogen werden
    int denominator = computeDenominator(edge.f2.getValue(), count);

    // Create two new edges with JaccardValue
    Edge jaccardEdge = new Edge();
    jaccardEdge.setId(GradoopId.get());
    jaccardEdge.setSourceId(edge.f0);
    jaccardEdge.setTargetId(edge.f1);
    jaccardEdge.setProperty(DEFAULT_JACCARD_EDGE_PROPERTY, (double) count / denominator);
    jaccardEdge.setLabel(edgeLabel);
    out.collect(jaccardEdge);

    Edge jaccardEdgeMirror = new Edge();
    jaccardEdgeMirror.setId(GradoopId.get());
    jaccardEdgeMirror.setSourceId(edge.f1);
    jaccardEdgeMirror.setTargetId(edge.f0);
    jaccardEdgeMirror.setProperty(DEFAULT_JACCARD_EDGE_PROPERTY, (double) count / denominator);
    jaccardEdgeMirror.setLabel(edgeLabel);
    out.collect(jaccardEdgeMirror);

  }

  private int computeDenominator(int value, int count) {

    // value - count equals the size of the union since common neighbors are counted double
    if(denominator.equals(Denominator.UNION)) {
      return value - count;
    } else {
      return value;
    }
  }
}