/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.matching.ASCIITemporalPatternMatchingTest;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.io.IOException;
import java.util.List;

/**
 * Uses citibike data to test matches. Base class for tests on isomorphism and homomorphism.
 */
public abstract class CBCypherTemporalPatternMatchingTest extends ASCIITemporalPatternMatchingTest {

  /**
   * Path to the default GDL file.
   */
  public static final String defaultData = "src/test/resources/data/patternmatchingtest/citibikesample";


  /**
   * Set the edge's {@code valid_from} and {@code tx_from} according to the {@code start}
   * property and the edge's {@code valid_to} and {@code tx_to} according to the
   * {@code end} property. Both properties are retained.
   */
  private final MapFunction<TemporalEdge, TemporalEdge> edgeTransform = new
    MapFunction<TemporalEdge, TemporalEdge>() {
      @Override
      public TemporalEdge map(TemporalEdge value) throws Exception {
        long start = value.getPropertyValue("start").getLong();
        long end = value.getPropertyValue("end").getLong();
        value.setValidTime(new Tuple2<>(start, end));
        value.setTransactionTime(value.getValidTime());
        //value.removeProperty("start");
        //value.removeProperty("end");
        return value;
      }
    };

  /**
   * Set the vertice's {@code valid_from} and {@code tx_from} according to the {@code start}
   * property and the vertice's {@code valid_to} and {@code tx_to} according to the
   * {@code end} property. Both properties are retained.
   */
  private final MapFunction<TemporalVertex, TemporalVertex> vertexTransform = new
    MapFunction<TemporalVertex, TemporalVertex>() {
      @Override
      public TemporalVertex map(TemporalVertex value) throws Exception {
        long start = value.getPropertyValue("start").getLong();
        long end = value.getPropertyValue("end").getLong();
        value.setValidTime(new Tuple2<>(start, end));
        value.setTransactionTime(value.getValidTime());
        //value.removeProperty("start");
        //value.removeProperty("end");
        return value;
      }
    };

  /**
   * initializes a test with a data graph
   *
   * @param testName               name of the test
   * @param queryGraph             the query graph as GDL-string
   * @param dataGraphPath          path to data graph file
   * @param expectedGraphVariables expected graph variables (names) as comma-separated string
   * @param expectedCollection     expected graph collection as comma-separated GDLs
   */
  public CBCypherTemporalPatternMatchingTest(String testName, String dataGraphPath, String queryGraph,
                                             String expectedGraphVariables, String expectedCollection) {
    super(testName, dataGraphPath, queryGraph, expectedGraphVariables, expectedCollection);
  }

  /**
   * Edits a typical query so that no default asOf(now) predicates are appended to
   * it during query processing.
   * This is achieved by appending a simple tx_to condition that should always be
   * true (if a tx_to condition is there, no default asOf()s are added)
   *
   * @param query a query containing a variable {@code e}, {@code e1} or {@code a}
   * @return query with practically same semantics but changed so that no default
   * asOfs are added
   */
  public static String prepareQueryString(String query) {
    /*boolean hasWhereClause = query.contains("WHERE");
    String middle = hasWhereClause? " AND " : " WHERE ";
    if (query.contains("[e]")) {
      return query + middle + " e.tx_to.after(1970-01-01)";
    } else if (query.contains("[e1]")) {
      return query + middle + " e1.tx_to.after(1970-01-01)";
    } else if (query.contains("(a)")) {
      return query + middle + " a.tx_to.after(1970-01-01)";
    }
    throw new IllegalArgumentException("Query must contain 'e', 'e1' or 'a'");*/
    return query;
  }

  @Override
  protected TemporalGraphCollection transformExpectedToTemporal(GraphCollection gc) throws Exception {
    //transform edges
    TemporalGraphCollection tgc = toTemporalGraphCollection(gc);
    DataSet<TemporalEdge> newEdges = tgc.getEdges().map(edgeTransform);
    DataSet<TemporalVertex> newVertices = tgc.getVertices().map(vertexTransform);
    tgc = tgc.getFactory().fromDataSets(tgc.getGraphHeads(), newVertices, newEdges);
    return tgc;
  }

  @Override
  protected TemporalGraph getTemporalGraphFromLoader(FlinkAsciiGraphLoader loader) throws Exception {
    try {
      loader.initDatabaseFromFile(dataGraphPath);
    } catch (IOException e) {
      e.printStackTrace();
    }
    LogicalGraph g = loader.getLogicalGraph();
    //new DOTDataSink("src/test/resources/data/patternmatchingtest/citibikesample.dot",true).write(g, true);
    return transformToTemporalGraph(g);
  }

  /**
   * Given a logical graph, this method transforms it to a temporal graph.
   * {@code start} and {@code end} values are extracted from the edges (= "trips")
   * and used to set {@code valid_from}/{@code tx_from} and {@code valid_to}/{@code tx_to}
   * values.
   *
   * @param g the logical graph to transform
   * @return logical graph transformed to temporal graph
   */
  private TemporalGraph transformToTemporalGraph(LogicalGraph g) throws Exception {
    TemporalGraph tg = toTemporalGraph(g);
    List<TemporalEdge> newEdges = tg.getEdges().map(edgeTransform).collect();
    List<TemporalVertex> newVertices = tg.getVertices().map(vertexTransform).collect();
    return tg.getFactory().fromCollections(newVertices, newEdges);
  }

  @Override
  protected FlinkAsciiGraphLoader getLoader() {
    return new FlinkAsciiGraphLoader(getConfig());
  }
}
