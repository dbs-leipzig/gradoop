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

package org.gradoop.model;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.helper.Aggregate;
import org.gradoop.model.helper.Algorithm;
import org.gradoop.model.helper.BinaryFunction;
import org.gradoop.model.helper.Order;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.SystemProperties;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.store.EPGraphStore;
import org.mockito.Mockito;

import java.util.Set;

public class WorkflowTest {

  public void summarizedCommunities() {
    EPGraphStore db = Mockito.mock(EPGraphStore.class);

    // read full graph from database
    EPGraph dbGraph = db.getDatabaseGraph();

    // extract friendships
    EPGraphCollection friendships =
      dbGraph.match("(a)-(c)->(b)", new Predicate<EPPatternGraph>() {
        @Override
        public boolean filter(EPPatternGraph graph) {
          return graph.getVertex("a").getLabel().equals("Person") &&
            graph.getEdge("c").getLabel().equals("knows") &&
            graph.getVertex("b").getLabel().equals("Person");
        }
      });

    // build single graph
    EPGraph knowsGraph =
      friendships.reduce(new BinaryFunction<EPGraph, EPGraph>() {
        @Override
        public EPGraph execute(EPGraph first, EPGraph second) {
          return first.combine(second);
        }
      });

    // apply label propagation
    knowsGraph = knowsGraph
      .callForGraph(Algorithm.LABEL_PROPAGATION, "propertyKey", "community");

    // summarize communities
    knowsGraph
      .summarize(Lists.newArrayList(SystemProperties.TYPE.name(), "city"),
        new Aggregate<Tuple2<EPVertexData, Set<EPVertexData>>, EPVertexData>() {
          @Override
          public EPVertexData aggregate(
            Tuple2<EPVertexData, Set<EPVertexData>> entity) {
            EPVertexData summarizedVertex = entity.f0;
            summarizedVertex.setProperty("count", entity.f1.size());
            return summarizedVertex;
          }
        }, Lists.newArrayList(SystemProperties.TYPE.name()),
        new Aggregate<Tuple2<EPEdgeData, Set<EPEdgeData>>, EPEdgeData>() {
          @Override
          public EPEdgeData aggregate(
            Tuple2<EPEdgeData, Set<EPEdgeData>> entity) {
            EPEdgeData summarizedEdge = entity.f0;
            summarizedEdge.setProperty("count", entity.f1.size());
            return summarizedEdge;
          }
        });
  }

  public void topRevenueBusinessProcess() throws Exception {
    EPGraphStore db = Mockito.mock(EPGraphStore.class);

    // read full graph from database
    EPGraph dbGraph = db.getDatabaseGraph();

    // extract business process instances
    EPGraphCollection btgs =
      dbGraph.callForCollection(Algorithm.BUSINESS_TRANSACTION_GRAPHS);

    // define predicate function (graph contains invoice)
    final Predicate<EPGraph> predicate = new Predicate<EPGraph>() {
      @Override
      public boolean filter(EPGraph graph) throws Exception {
        return graph.getVertices().filter(new Predicate<EPVertexData>() {
          @Override
          public boolean filter(EPVertexData entity) {
            return entity.getLabel().equals("SalesInvoice");
          }
        }).size() > 0;
      }
    };

    // define aggregate function (revenue per graph)
    final Aggregate<EPGraph, Double> aggregateFunc =
      new Aggregate<EPGraph, Double>() {
        @Override
        public Double aggregate(EPGraph entity) {
          Double sum = 0.0;
          for (Double v : entity.getVertices()
            .values(Double.class, "revenue")) {
            sum += v;
          }
          return sum;
        }
      };

    // apply predicate and aggregate function
    EPGraphCollection invBtgs =
      btgs.select(predicate).apply(new UnaryFunction<EPGraph, EPGraph>() {
        @Override
        public EPGraph execute(EPGraph entity) {
          return entity.aggregate("revenue", aggregateFunc);
        }
      });

    // sort graphs by revenue and return top 100
    EPGraphCollection topBTGs =
      invBtgs.sortBy("revenue", Order.DESCENDING).top(100);

    // compute overlap to find master store objects (e.g. Employee)
    EPGraph topOverlap = topBTGs.reduce(new BinaryFunction<EPGraph, EPGraph>() {
      @Override
      public EPGraph execute(EPGraph first, EPGraph second) {
        return first.combine(second);
      }
    });
  }

  public void clusterCharacteristicPatterns() throws Exception {
    EPGraphStore db = Mockito.mock(EPGraphStore.class);

    // generate base collection
    EPGraphCollection btgs = db.getDatabaseGraph()
      .callForCollection(Algorithm.BUSINESS_TRANSACTION_GRAPHS);

    // define aggregate function (profit per graph)
    final Aggregate<EPGraph, Double> aggFunc =
      new Aggregate<EPGraph, Double>() {
        @Override
        public Double aggregate(EPGraph entity) {
          Double revenue = 0.0;
          Double expense = 0.0;
          for (Double v : entity.getVertices()
            .values(Double.class, "revenue")) {
            revenue += v;
          }
          for (Double v : entity.getVertices()
            .values(Double.class, "expense")) {
            expense += v;
          }
          return revenue - expense;
        }
      };

    // apply aggregate function on btgs
    btgs = btgs.apply(new UnaryFunction<EPGraph, EPGraph>() {
      @Override
      public EPGraph execute(EPGraph entity) {
        return entity.aggregate("profit", aggFunc);
      }
    });

    // vertex function for projection
    final UnaryFunction<EPVertexData, EPVertexData> vertexFunc =
      new UnaryFunction<EPVertexData, EPVertexData>() {
        @Override
        public EPVertexData execute(EPVertexData entity) {
          EPVertexData newVertex = Mockito.mock(EPVertexData.class);
          if ((Boolean) entity.getProperty("IsMasterData")) {
            newVertex.setLabel(entity.getProperty("sourceID").toString());
          } else {
            newVertex.setLabel(entity.getLabel());
          }
          newVertex.setProperty("result", entity.getProperty("result"));
          return newVertex;
        }
      };

    // edge function for projection
    final UnaryFunction<EPEdgeData, EPEdgeData> edgeFunc =
      new UnaryFunction<EPEdgeData, EPEdgeData>() {
        @Override
        public EPEdgeData execute(EPEdgeData entity) {
          EPEdgeData newEdge = Mockito.mock(EPEdgeData.class);
          newEdge.setLabel(entity.getLabel());
          return newEdge;
        }
      };

    // apply projection on all btgs
    btgs = btgs.apply(new UnaryFunction<EPGraph, EPGraph>() {
      @Override
      public EPGraph execute(EPGraph entity) {
        return entity.project(vertexFunc, edgeFunc);
      }
    });

    // select profit and loss clusters
    EPGraphCollection profitBtgs = btgs.filter(new Predicate<EPGraphData>() {
      @Override
      public boolean filter(EPGraphData entity) {
        return (Double) entity.getProperty("result") >= 0;
      }
    });
    EPGraphCollection lossBtgs = btgs.difference(profitBtgs);

    EPGraphCollection profitFreqPats = profitBtgs
      .callForCollection(Algorithm.FREQUENT_SUBGRAPHS, "threshold", "0.7");

    EPGraphCollection lossFreqPats = lossBtgs
      .callForCollection(Algorithm.FREQUENT_SUBGRAPHS, "threshold", "0.7");

    // determine cluster characteristic patterns
    EPGraphCollection trivialPats = profitFreqPats.intersect(lossFreqPats);
    EPGraphCollection profitCharPatterns =
      profitFreqPats.difference(trivialPats);
    EPGraphCollection lossCharPatterns = lossFreqPats.difference(trivialPats);
  }
}
