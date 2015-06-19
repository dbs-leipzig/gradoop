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
import javafx.util.Pair;
import org.gradoop.model.helper.Aggregate;
import org.gradoop.model.helper.Algorithm;
import org.gradoop.model.helper.BinaryFunction;
import org.gradoop.model.helper.Order;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.SystemProperties;
import org.gradoop.model.helper.UnaryFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class WorkflowTest {

  @Test
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
        new Aggregate<Pair<Vertex, Set<Vertex>>, Vertex>() {
          @Override
          public Vertex aggregate(Pair<Vertex, Set<Vertex>> entity) {
            Vertex summarizedVertex = entity.getKey();
            summarizedVertex.addProperty("count", entity.getValue().size());
            return summarizedVertex;
          }
        }, Lists.newArrayList(SystemProperties.TYPE.name()),
        new Aggregate<Pair<Edge, Set<Edge>>, Edge>() {
          @Override
          public Edge aggregate(Pair<Edge, Set<Edge>> entity) {
            Edge summarizedEdge = entity.getKey();
            summarizedEdge.addProperty("count", entity.getValue().size());
            return summarizedEdge;
          }
        });

    // store the resulting summarized graph
    db.writeGraph(knowsGraph);
  }

  @Test
  public void topRevenueBusinessProcess() {
    EPGraphStore db = Mockito.mock(EPGraphStore.class);

    // read full graph from database
    EPGraph dbGraph = db.getDatabaseGraph();

    // extract business process instances
    EPGraphCollection btgs =
      dbGraph.callForCollection(Algorithm.BUSINESS_TRANSACTION_GRAPHS);

    // define predicate function (graph contains invoice)
    final Predicate<EPGraph> predicate = new Predicate<EPGraph>() {
      @Override
      public boolean filter(EPGraph graph) {
        return graph.getVertices().select(new Predicate<Vertex>() {
          @Override
          public boolean filter(Vertex entity) {
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

    // compute overlap to find master data objects (e.g. Employee)
    EPGraph topOverlap = topBTGs.reduce(new BinaryFunction<EPGraph, EPGraph>() {
      @Override
      public EPGraph execute(EPGraph first, EPGraph second) {
        return first.combine(second);
      }
    });
  }

  @Test
  public void clusterCharacteristicPatterns() {
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
    final UnaryFunction<Vertex, Vertex> vertexFunc =
      new UnaryFunction<Vertex, Vertex>() {
        @Override
        public Vertex execute(Vertex entity) {
          Vertex newVertex = Mockito.mock(Vertex.class);
          if ((Boolean) entity.getProperty("IsMasterData")) {
            newVertex.setLabel(entity.getProperty("sourceID").toString());
          } else {
            newVertex.setLabel(entity.getLabel());
          }
          newVertex.addProperty("result", entity.getProperty("result"));
          return newVertex;
        }
      };

    // edge function for projection
    final UnaryFunction<Edge, Edge> edgeFunc = new UnaryFunction<Edge, Edge>() {
      @Override
      public Edge execute(Edge entity) {
        Edge newEdge = Mockito.mock(Edge.class);
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
    EPGraphCollection profitBtgs = btgs.select(new Predicate<EPGraph>() {
      @Override
      public boolean filter(EPGraph entity) {
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
