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

package org.gradoop.io.impl.tlf.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains different sublcasses which are all needed to map TLF
 * dictionaries to a TLF graph.
 */
public class TLFDictionaryFunctions {

  /**
   * After the TLF dictionary file has been read with a normal text input
   * format its result text has to be split and formed into Tuple2<Integer,
   * String>.
   */
  public static class TLFDictionaryStringToTupleMapper implements
    MapFunction<Tuple2<LongWritable, Text>, Tuple2<Integer, String>> {

    /**
     * Creates a tuple of integer and string from the input text.
     *
     * @param tuple tuple received from TextInputFormat, for each line
     * @return tuple of the text, which was split into integer and string
     * @throws Exception
     */
    @Override
    public Tuple2<Integer, String> map(
      Tuple2<LongWritable, Text> tuple) throws Exception {
      String label;
      Integer id;
      String[] stringArray;

      stringArray = tuple.getField(1).toString().split(" ");
      id = Integer.parseInt(stringArray[1]);
      label = stringArray[0];

      return new Tuple2<>(id, label);
    }
  }

  /**
   * Converts a dataset of tuples of integer and string into a dataset which
   * contains only one map from integer to string.
   */
  public static class TLFDictionaryTupleToMapGroupReducer implements
    GroupReduceFunction<Tuple2<Integer, String>, Map<Integer,
      String>> {

    /**
     * Reduces the Tuple2 iterable into one map.
     *
     * @param iterable containing tuples of integer and string
     * @param collector collects one map from integer to string
     * @throws Exception
     */
    @Override
    public void reduce(
      Iterable<Tuple2<Integer, String>> iterable,
      Collector<Map<Integer, String>> collector) throws Exception {
      Map<Integer, String> dictionary = new HashMap<>();
      for (Tuple2<Integer, String> tuple : iterable) {
        dictionary.put((Integer) tuple.getField(0), (String) tuple
          .getField(1));
      }
      collector.collect(dictionary);
    }
  }

  /**
   * Maps the vertex dictionary or the edge dictionary or both to a given
   * graph transaction. The integer-like labels are replaced by those from
   * the dictionary files where the integer value from the old labels matches
   * the corresponding keys from the dictionary.
   *
   * @param <G> EPGM graph head type
   * @param <V> EPGM vertex type
   * @param <E> EPGM edge type
   */
  public static class GraphTransactionDictionaryMapper<G extends
    EPGMGraphHead,
    V extends EPGMVertex, E extends EPGMEdge> extends
    RichMapFunction<GraphTransaction<G, V, E>, GraphTransaction<G, V, E>> {
    /**
     * Constant for broadcast set containing the vertex dictionary.
     */
    public static final String VERTEX_DICTIONARY = "vertexDictionary";
    /**
     * Constant for broadcast set containing the edge dictionary.
     */
    public static final String EDGE_DICTIONARY = "edgeDictionary";
    /**
     * Constant string which is added to those edges or vertices which do not
     * have an entry in the dictionary while others have one.
     */
    private static final String NO_LABEL = " - no label";
    /**
     * Map which contains a vertex dictionary.
     */
    private Map<Integer, String> vertexDictionary;
    /**
     * Map which contains a edge dictionary.
     */
    private Map<Integer, String> edgeDictionary;
    /**
     * Is set to true if a vertex dictionary is set as broadcast.
     */
    private boolean mapVertexLabels;
    /**
     * Is set to true if a edge dictionary is set as broadcast.
     */
    private boolean mapEdgeLabels;

    /**
     * Creates a map function for labels read from a dictionary file.
     *
     * @param mapVertexLabels set to true if vertex dictionary is set as
     *                        broadcast
     * @param mapEdgeLabels set to true if edge dictionary is set as broadcast
     */
    public GraphTransactionDictionaryMapper(boolean mapVertexLabels,
      boolean mapEdgeLabels) {
      this.mapVertexLabels = mapVertexLabels;
      this.mapEdgeLabels = mapEdgeLabels;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      if (mapVertexLabels) {
        vertexDictionary = getRuntimeContext()
          .<HashMap<Integer, String>>getBroadcastVariable(VERTEX_DICTIONARY)
          .get(0);

      }
      if (mapEdgeLabels) {
        edgeDictionary = getRuntimeContext()
          .<HashMap<Integer, String>>getBroadcastVariable(EDGE_DICTIONARY)
          .get(0);
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTransaction<G, V, E> map(
      GraphTransaction<G, V, E> graphTransaction) throws Exception {
      String label;
      if (mapVertexLabels) {
        for (V vertex : graphTransaction.getVertices()) {
          label = vertexDictionary.get(Integer.parseInt(vertex.getLabel()));
          if (label != null) {
            vertex.setLabel(label);
          } else {
            vertex.setLabel(vertex.getLabel() + NO_LABEL);
          }
        }
      }
      if (mapEdgeLabels) {
        for (E edge : graphTransaction.getEdges()) {
          label = edgeDictionary.get(Integer.parseInt(edge.getLabel()));
          if (label != null) {
            edge.setLabel(label);
          } else {
            edge.setLabel(edge.getLabel() + NO_LABEL);
          }
        }
      }
      return graphTransaction;
    }
  }
}
