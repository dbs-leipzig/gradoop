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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.collection;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.api.operators.BinaryCollectionToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;

import java.util.Iterator;

/**
 * Abstract operator implementation which can be used with binary collection
 * to collection operators.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public abstract class AbstractBinaryCollectionToCollectionOperator<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData>
  implements BinaryCollectionToCollectionOperator<VD, ED, GD> {

  /**
   * First input collection.
   */
  protected GraphCollection<VD, ED, GD> firstCollection;
  /**
   * Second input collection.
   */
  protected GraphCollection<VD, ED, GD> secondCollection;

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphCollection<VD, ED, GD> execute(
    GraphCollection<VD, ED, GD> firstCollection,
    GraphCollection<VD, ED, GD> secondCollection) throws Exception {

    // do some init stuff for the actual operator
    this.firstCollection = firstCollection;
    this.secondCollection = secondCollection;

    final DataSet<GD> newGraphHeads = computeNewGraphHeads();
    final DataSet<VD> newVertices = computeNewVertices(newGraphHeads);
    final DataSet<ED> newEdges = computeNewEdges(newVertices);

    return new GraphCollection<>(newVertices, newEdges, newGraphHeads,
      firstCollection.getConfig());
  }

  /**
   * Overridden by inheriting classes.
   *
   * @param newGraphHeads new graph heads
   * @return vertex set of the resulting graph collection
   */
  protected abstract DataSet<VD> computeNewVertices(
    DataSet<GD> newGraphHeads) throws Exception;

  /**
   * Overridden by inheriting classes.
   *
   * @return subgraph dataset of the resulting collection
   */
  protected abstract DataSet<GD> computeNewGraphHeads();

  /**
   * Overridden by inheriting classes.
   *
   * @param newVertices vertex set of the resulting graph collection
   * @return edges set only connect vertices in {@code newVertices}
   */
  protected abstract DataSet<ED> computeNewEdges(DataSet<VD> newVertices);

  /**
   * Checks if the number of grouped elements equals a given expected size.
   *
   * @param <GD> EPGM graph head type
   * @see Intersect
   */
  protected static class GraphHeadGroupReducer<GD extends GraphData> implements
    GroupReduceFunction<GD, GD> {

    /**
     * User defined expectedGroupSize.
     */
    private final long expectedGroupSize;

    /**
     * Creates new group reducer.
     *
     * @param expectedGroupSize expected group size
     */
    public GraphHeadGroupReducer(long expectedGroupSize) {
      this.expectedGroupSize = expectedGroupSize;
    }

    /**
     * If the number of elements in the group is equal to the user expected
     * group size, the subgraph will be returned.
     *
     * @param iterable  graph data
     * @param collector output collector (contains 0 or 1 graph)
     * @throws Exception
     */
    @Override
    public void reduce(Iterable<GD> iterable,
      Collector<GD> collector) throws Exception {
      Iterator<GD> iterator = iterable.iterator();
      long count = 0L;
      GD graphHead = null;
      while (iterator.hasNext()) {
        graphHead = iterator.next();
        count++;
      }
      if (count == expectedGroupSize) {
        collector.collect(graphHead);
      }
    }
  }

  /**
   * Creates a {@link Tuple2} from the given input and a given Long value.
   *
   * @param <C> input type
   */
  @FunctionAnnotation.ForwardedFields("*->f0")
  protected static class Tuple2LongMapper<C> implements
    MapFunction<C, Tuple2<C, Long>> {

    /**
     * Value to add to the resulting tuple.
     */
    private final Long secondField;

    /**
     * Creates this mapper
     *
     * @param secondField user defined long value
     */
    public Tuple2LongMapper(Long secondField) {
      this.secondField = secondField;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple2<C, Long> map(C c) throws Exception {
      return new Tuple2<>(c, secondField);
    }
  }

  /**
   * Returns the identifier of the subgraph in the given tuple.
   *
   * @param <GD> graph data type
   * @param <C>  type of second element in tuple
   */
  protected static class SubgraphTupleKeySelector<GD extends GraphData, C>
    implements
    KeySelector<Tuple2<GD, C>, Long> {
    /**
     * {@inheritDoc}
     */
    @Override
    public Long getKey(Tuple2<GD, C> subgraph) throws
      Exception {
      return subgraph.f0.getId();
    }
  }
}
