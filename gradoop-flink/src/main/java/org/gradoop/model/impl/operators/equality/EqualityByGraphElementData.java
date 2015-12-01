package org.gradoop.model.impl.operators.equality;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.bool.And;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.functions.bool.Or;
import org.gradoop.model.impl.functions.join.LeftSide;
import org.gradoop.model.impl.functions.counting.Tuple1With1L;
import org.gradoop.model.impl.operators.equality.functions.DataLabelWithCount;
import org.gradoop.model.impl.operators.equality.functions.EdgeDataLabeler;
import org.gradoop.model.impl.operators.equality.functions.GraphHeadDataLabeler;
import org.gradoop.model.impl.operators.equality.functions.LabelAppender;
import org.gradoop.model.impl.operators.equality.functions.SortAndConcatLabels;
import org.gradoop.model.impl.operators.equality.functions.SourceLabelAppender;
import org.gradoop.model.impl.operators.equality.functions.TargetLabelAppender;
import org.gradoop.model.impl.operators.equality.functions.VertexDataLabeler;
import org.gradoop.model.impl.operators.equality.tuples.DataLabel;
import org.gradoop.model.impl.operators.equality.tuples.EdgeDataLabel;

/**
 * Two collections are equal,
 * if there exists an 1:1 mapping between graphs, where for each pair
 * there exists an isomorphism based on element label and property equality.
 *
 * @param <G> graph head type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class EqualityByGraphElementData
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends EqualityBase<G, V, E>
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

    try {
      Equals.cross(distinctFirstGraphCount, matchingIdCount);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return Or.union(
      And.cross(firstCollection.isEmpty(), secondCollection.isEmpty()),
      Equals.cross(distinctFirstGraphCount, matchingIdCount)
    );
  }

  /**
   * Returns a dataset containing canonical labels for all graphs of an input
   * collection.
   *
   * @param collection input collection
   * @return canonical labels
   */
  private DataSet<Tuple2<String, Long>> labelGraphs(
    GraphCollection<G, V, E> collection) {

    DataSet<DataLabel> graphHeadLabels = collection.getGraphHeads()
      .map(new GraphHeadDataLabeler<G>());

    DataSet<DataLabel> vertexLabels = collection.getVertices()
      .flatMap(new VertexDataLabeler<V>())
      .join(graphHeadLabels)
      .where(0).equalTo(1)
      .with(new LeftSide<DataLabel, DataLabel>());

    DataSet<EdgeDataLabel> edgeLabels = collection.getEdges()
      .flatMap(new EdgeDataLabeler<E>())
      .groupBy(0, 1, 2)
      .reduceGroup(new SortAndConcatLabels<EdgeDataLabel>());

    DataSet<DataLabel> outgoingEdgeLabels = edgeLabels
      .join(vertexLabels)
      .where(0, 2).equalTo(0, 1)
      .with(new TargetLabelAppender())
      .groupBy(0, 1)
      .reduceGroup(new SortAndConcatLabels<DataLabel>());

    DataSet<DataLabel> incomingEdgeLabels = edgeLabels
      .join(vertexLabels)
      .where(0, 1).equalTo(0, 1)
      .with(new SourceLabelAppender())
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
      .map(new DataLabelWithCount())
      .groupBy(0)
      .sum(1);
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
