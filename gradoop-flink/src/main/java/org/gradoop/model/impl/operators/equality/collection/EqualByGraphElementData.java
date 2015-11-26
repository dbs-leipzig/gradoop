package org.gradoop.model.impl.operators.equality.collection;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.LeftSideOnly;
import org.gradoop.model.impl.functions.counting.Tuple1With1L;
import org.gradoop.model.impl.operators.equality.EqualityBase;
import org.gradoop.model.impl.operators.equality.functions
  .CanonicalLabelWithCount;
import org.gradoop.model.impl.operators.equality.functions.EdgeDataLabeler;
import org.gradoop.model.impl.operators.equality.functions.GraphHeadDataLabeler;
import org.gradoop.model.impl.operators.equality.functions.LabelAppender;
import org.gradoop.model.impl.operators.equality.functions.SortAndConcatLabels;
import org.gradoop.model.impl.operators.equality.functions
  .SourceVertexLabelAppender;
import org.gradoop.model.impl.operators.equality.functions.TargetVertexLabelAppender;
import org.gradoop.model.impl.operators.equality.functions.VertexDataLabeler;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;
import org.gradoop.model.impl.operators.equality.tuples.EdgeDataLabel;

/**
 * Created by peet on 19.11.15.
 */
public class EqualByGraphElementData
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends EqualityBase
  implements BinaryCollectionToValueOperator<G, V, E, Boolean> {

  @Override
  public DataSet<Boolean> execute(GraphCollection<G, V, E> firstCollection,
    GraphCollection<G, V, E> secondCollection) {

    DataSet<Tuple2<String, Long>> firstGraphLabels =
      labelGraphs(firstCollection);
    DataSet<Tuple2<String, Long>> secondGraphLabels =
      labelGraphs(secondCollection);

    DataSet<Tuple1<Long>> distinctFirstGraphCount = firstGraphLabels
      .map(new Tuple1With1L<Tuple2<String, Long>>())
      .sum(0);

    DataSet<Tuple1<Long>> matchingIdCount = firstGraphLabels
      .join(secondGraphLabels)
      .where(0, 1).equalTo(0, 1)
      .with(new Tuple1With1L<Tuple2<String, Long>>())
      .sum(0);

    return checkCountEqualsCount(distinctFirstGraphCount, matchingIdCount);
  }

  private DataSet<Tuple2<String, Long>> labelGraphs(
    GraphCollection<G, V, E> collection) {

    DataSet<DataLabel> graphHeadLabels = collection.getGraphHeads()
      .map(new GraphHeadDataLabeler<G>());

    DataSet<DataLabel> vertexLabels = collection.getVertices()
      .flatMap(new VertexDataLabeler<V>())
      .join(graphHeadLabels)
      .where(0).equalTo(1)
      .with(new LeftSideOnly<DataLabel, DataLabel>());

    DataSet<EdgeDataLabel> edgeLabels = collection.getEdges()
      .flatMap(new EdgeDataLabeler<E>())
      .groupBy(0, 1, 2)
      .reduceGroup(new SortAndConcatLabels<EdgeDataLabel>());

    DataSet<DataLabel> outgoingEdgeLabels = edgeLabels
      .join(vertexLabels)
      .where(0, 2).equalTo(0, 1)
      .with(new TargetVertexLabelAppender())
      .groupBy(0, 1)
      .reduceGroup(new SortAndConcatLabels<DataLabel>());

    DataSet<DataLabel> incomingEdgeLabels = edgeLabels
      .join(vertexLabels)
      .where(0, 1).equalTo(0, 1)
      .with(new SourceVertexLabelAppender())
      .groupBy(0, 1)
      .reduceGroup(new SortAndConcatLabels<DataLabel>());

    DataSet<DataLabel> graphDataLabels = vertexLabels
      .leftOuterJoin(outgoingEdgeLabels)
      .where(0, 1).equalTo(0, 1)
      .with(new LabelAppender())
      .leftOuterJoin(incomingEdgeLabels)
      .where(0, 1).equalTo(0, 1)
      .with(new LabelAppender())
      .groupBy(0)
      .reduceGroup(new SortAndConcatLabels<DataLabel>());

    return graphHeadLabels
      .join(graphDataLabels)
      .where(1).equalTo(0)
      .with(new LabelAppender())
      .map(new CanonicalLabelWithCount())
      .groupBy(0)
      .sum(1);
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
