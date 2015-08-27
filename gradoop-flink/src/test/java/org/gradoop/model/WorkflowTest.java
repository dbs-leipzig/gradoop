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

import org.apache.commons.lang.NotImplementedException;
import org.gradoop.model.helper.Order;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.DefaultEdgeData;
import org.gradoop.model.impl.DefaultGraphData;
import org.gradoop.model.impl.DefaultVertexData;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.Aggregation;
import org.gradoop.model.impl.operators.Combination;
import org.gradoop.model.impl.operators.Projection;
import org.gradoop.model.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;
import org.mockito.Mockito;

@SuppressWarnings({"unchecked", "UnusedAssignment"})
public abstract class WorkflowTest {

  public void summarizedCommunities() throws Exception {
    EPGMDatabase db = Mockito.mock(EPGMDatabase.class);

    // read full graph from database
    LogicalGraph dbGraph = db.getDatabaseGraph();

    // extract friendships
    GraphCollection friendships =
      dbGraph.match("(a)-(c)->(b)", new Predicate<PatternGraph>() {
        @Override
        public boolean filter(PatternGraph graph) {
          return graph.getVertex("a").getLabel().equals("Person") &&
            graph.getEdge("c").getLabel().equals("knows") &&
            graph.getVertex("b").getLabel().equals("Person");
        }
      });

    // build single graph
    LogicalGraph knowsGraph = friendships.reduce(new Combination());

    // apply label propagation
    GraphCollection communities =
      knowsGraph.callForCollection(new LP("communityID"));
    // and build one graph
    knowsGraph = communities.reduce(new Combination());

    // summarize communities
    knowsGraph = knowsGraph.summarize("city", "since");
  }

  public void topRevenueBusinessProcess() throws Exception {
    EPGMDatabase db = Mockito.mock(EPGMDatabase.class);

    // read full graph from database
    LogicalGraph dbGraph = db.getDatabaseGraph();

    // extract business process instances
    GraphCollection btgs = dbGraph.callForCollection(new BTG());

    // define predicate function (graph contains invoice)
    final Predicate<LogicalGraph> predicate = new Predicate<LogicalGraph>() {
      @Override
      public boolean filter(LogicalGraph graph) throws Exception {
        return graph.getVertices().filter(new Predicate<VertexData>() {
          @Override
          public boolean filter(VertexData entity) {
            return entity.getLabel().equals("SalesInvoice");
          }
        }).size() > 0;
      }
    };

    // define aggregate function (revenue per graph)
    final UnaryFunction<LogicalGraph<DefaultVertexData, DefaultEdgeData,
      DefaultGraphData>, Double>
      aggregateFunc = null;
    new UnaryFunction<LogicalGraph<DefaultVertexData, DefaultEdgeData,
      DefaultGraphData>, Double>() {
      @Override
      public Double execute(
        LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
          entity) {
        Double sum = 0.0;
        for (Double v : entity.getVertices().values(Double.class, "revenue")) {
          sum += v;
        }
        return sum;
      }
    };

    // apply predicate and aggregate function
    GraphCollection invBtgs =
      btgs.select(predicate).apply(new Aggregation<>("revenue", aggregateFunc));

    // sort graphs by revenue and return top 100
    GraphCollection topBTGs =
      invBtgs.sortBy("revenue", Order.DESCENDING).top(100);

    // compute overlap to find master store objects (e.g. Employee)
    LogicalGraph topOverlap = topBTGs.reduce(new Combination());
  }

  public void clusterCharacteristicPatterns() throws Exception {
    EPGMDatabase db = Mockito.mock(EPGMDatabase.class);

    // generate base collection
    GraphCollection btgs = db.getDatabaseGraph()
      .callForCollection(new UnaryGraphToCollectionOperator() {
        @Override
        public GraphCollection execute(LogicalGraph graph) {
          // TODO execute BTG Computation
          throw new NotImplementedException();
        }

        @Override
        public String getName() {
          return "BTGComputation";
        }
      });

    // define aggregate function (profit per graph)
    final UnaryFunction<LogicalGraph<DefaultVertexData, DefaultEdgeData,
      DefaultGraphData>, Double>
      aggFunc = null;
    new UnaryFunction<LogicalGraph<DefaultVertexData, DefaultEdgeData,
      DefaultGraphData>, Double>() {
      @Override
      public Double execute(
        LogicalGraph<DefaultVertexData, DefaultEdgeData, DefaultGraphData>
          entity) {
        Double revenue = 0.0;
        Double expense = 0.0;
        for (Double v : entity.getVertices().values(Double.class, "revenue")) {
          revenue += v;
        }
        for (Double v : entity.getVertices().values(Double.class, "expense")) {
          expense += v;
        }
        return revenue - expense;
      }
    };

    // apply aggregate function on btgs
    btgs = btgs.apply(new Aggregation<>("profit", aggFunc));

    // vertex function for projection
    final UnaryFunction<VertexData, VertexData> vertexFunc =
      new UnaryFunction<VertexData, VertexData>() {
        @Override
        public VertexData execute(VertexData entity) {
          VertexData newVertex = Mockito.mock(VertexData.class);
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
    final UnaryFunction<EdgeData, EdgeData> edgeFunc =
      new UnaryFunction<EdgeData, EdgeData>() {
        @Override
        public EdgeData execute(EdgeData entity) {
          EdgeData newEdge = Mockito.mock(EdgeData.class);
          newEdge.setLabel(entity.getLabel());
          return newEdge;
        }
      };

    // apply projection on all btgs
    btgs = btgs.apply(new Projection(vertexFunc, edgeFunc));

    // select profit and loss clusters
    GraphCollection profitBtgs = btgs.filter(new Predicate<GraphData>() {
      @Override
      public boolean filter(GraphData entity) {
        return (Double) entity.getProperty("result") >= 0;
      }
    });
    GraphCollection lossBtgs = btgs.difference(profitBtgs);

    GraphCollection profitFreqPats =
      profitBtgs.callForCollection(new FSM(0.7f));
    GraphCollection lossFreqPats = lossBtgs.callForCollection(new FSM(0.7f));

    // determine cluster characteristic patterns
    GraphCollection trivialPats = profitFreqPats.intersect(lossFreqPats);
    GraphCollection profitCharPatterns = profitFreqPats.difference(trivialPats);
    GraphCollection lossCharPatterns = lossFreqPats.difference(trivialPats);
  }

  private static class FSM implements UnaryCollectionToCollectionOperator {

    private final float threshold;

    public FSM(float threshold) {

      this.threshold = threshold;
    }

    @Override
    public String getName() {
      return "FSM";
    }

    @Override
    public GraphCollection execute(GraphCollection collection) {
      throw new NotImplementedException();
    }
  }

  private static class LP implements UnaryGraphToCollectionOperator {

    private final String propertyKey;

    public LP(String propertyKey) {

      this.propertyKey = propertyKey;
    }

    @Override
    public GraphCollection execute(LogicalGraph graph) {
      throw new NotImplementedException();
    }

    @Override
    public String getName() {
      return "LabelPropagation";
    }
  }

  private static class BTG implements UnaryGraphToCollectionOperator {

    @Override
    public GraphCollection execute(LogicalGraph graph) {
      // TODO execute BTG Computation
      throw new NotImplementedException();
    }

    @Override
    public String getName() {
      return "BTGComputation";
    }
  }

  private interface PatternGraph {

    VertexData getVertex(String variable);

    EdgeData getEdge(String variable);
  }
}
