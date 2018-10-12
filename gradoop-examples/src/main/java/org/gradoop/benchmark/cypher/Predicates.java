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
package org.gradoop.benchmark.cypher;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.impl.hbase.predicate.filter.api.HBaseElementFilter;
import org.gradoop.storage.utils.HBaseFilters;

/**
 * Used Predicates for {@link CypherBenchmark}. Inner static classes offer the possibility to define
 * predicates for HBase and Accumulo.
 */
public class Predicates {
  /**
   * HBase predicates
   */
  public static class HBase {
    /**
     * Vertex Filter for HBase DataSource of Query {@link Queries#q1(String)}.
     *
     * @param name used first name in query
     * @return filter implementation
     */
    static ElementQuery<HBaseElementFilter<Vertex>> v1(String name) {
      return Query.elements().fromAll()
        .where(HBaseFilters.<Vertex>labelIn("person")
          .and(HBaseFilters.propEquals("firstName", name))
          .or(HBaseFilters.labelIn("comment", "post")));
    }

    /**
     * Edge Filter for HBase DataSource of Query {@link Queries#q1(String)}.
     *
     * @return filter implementation
     */
    static ElementQuery<HBaseElementFilter<Edge>> e1() {
      return Query.elements().fromAll().where(HBaseFilters.labelIn("hasCreator"));
    }

    /**
     * Vertex Filter for HBase DataSource of Query {@link Queries#q2(String)}.
     *
     * @param name used first name in query
     * @return filter implementation
     */
    static ElementQuery<HBaseElementFilter<Vertex>> v2(String name) {
      return v1(name);
    }

    /**
     * Edge Filter for HBase DataSource of Query {@link Queries#q2(String)}.
     *
     * @return filter implementation
     */
    static ElementQuery<HBaseElementFilter<Edge>> e2() {
      return Query.elements().fromAll().where(HBaseFilters.labelIn("hasCreator", "replyOf"));
    }

    /**
     * Vertex Filter for HBase DataSource of Query {@link Queries#q3(String)}.
     *
     * @param name used first name in query
     * @return filter implementation
     */
    static ElementQuery<HBaseElementFilter<Vertex>> v3(String name) {
      return v1(name);
    }

    /**
     * Edge Filter for HBase DataSource of Query {@link Queries#q3(String)}.
     *
     * @return filter implementation
     */
    static ElementQuery<HBaseElementFilter<Edge>> e3() {
      return e2();
    }

    /**
     * Vertex Filter for HBase DataSource of Query {@link Queries#q4()}.
     *
     * @return filter implementation
     */
    static ElementQuery<HBaseElementFilter<Vertex>> v4() {
      return Query.elements().fromAll()
        .where(HBaseFilters.labelIn("person", "city", "tag", "university", "forum"));
    }

    /**
     * Edge Filter for HBase DataSource of Query {@link Queries#q4()}.
     *
     * @return filter implementation
     */
    static ElementQuery<HBaseElementFilter<Edge>> e4() {
      return Query.elements().fromAll()
        .where(HBaseFilters.labelIn("isLocatedIn", "hasInterest", "studyAt", "hasMember",
          "hasModerator"));
    }

    /**
     * Vertex Filter for HBase DataSource of Query {@link Queries#q5()}.
     *
     * @return filter implementation
     */
    static ElementQuery<HBaseElementFilter<Vertex>> v5() {
      return Query.elements().fromAll().where(HBaseFilters.labelIn("person"));
    }

    /**
     * Edge Filter for HBase DataSource of Query {@link Queries#q5()}.
     *
     * @return filter implementation
     */
    static ElementQuery<HBaseElementFilter<Edge>> e5() {
      return Query.elements().fromAll().where(HBaseFilters.labelIn("knows"));
    }

    /**
     * Vertex Filter for HBase DataSource of Query {@link Queries#q6()}.
     *
     * @return filter implementation
     */
    static ElementQuery<HBaseElementFilter<Vertex>> v6() {
      return Query.elements().fromAll().where(HBaseFilters.labelIn("person", "tag"));
    }

    /**
     * Edge Filter for HBase DataSource of Query {@link Queries#q6()}.
     *
     * @return filter implementation
     */
    static ElementQuery<HBaseElementFilter<Edge>> e6() {
      return Query.elements().fromAll().where(HBaseFilters.labelIn("knows", "hasInterest"));
    }

    /**
     * Returns a vertex filter predicate which fits to the given query. It can be used as
     * store predicate.
     *
     * @param query the query string ('q1', 'q2', ..., 'q6')
     * @param firstName the name to be used in the queries q1, q2 and q3
     * @return HBase vertex filter
     */
    static ElementQuery<HBaseElementFilter<Vertex>> getVertexFilter(String query,
      String firstName) {
      switch (query) {
      case "q1" : return Predicates.HBase.v1(firstName);
      case "q2" : return Predicates.HBase.v2(firstName);
      case "q3" : return Predicates.HBase.v3(firstName);
      case "q4" : return Predicates.HBase.v4();
      case "q5" : return Predicates.HBase.v5();
      case "q6" : return Predicates.HBase.v6();
      default : throw new IllegalArgumentException("Unsupported query: " + query);
      }
    }

    /**
     * Returns a edge filter predicate which fits to the given query. It can be used as
     * store predicate.
     *
     * @param query the query string ('q1', 'q2', ..., 'q6')
     * @return HBase edge filter
     */
    static ElementQuery<HBaseElementFilter<Edge>> getEdgeFilter(String query) {
      switch (query) {
      case "q1" : return Predicates.HBase.e1();
      case "q2" : return Predicates.HBase.e2();
      case "q3" : return Predicates.HBase.e3();
      case "q4" : return Predicates.HBase.e4();
      case "q5" : return Predicates.HBase.e5();
      case "q6" : return Predicates.HBase.e6();
      default : throw new IllegalArgumentException("Unsupported query: " + query);
      }
    }
  }
}
