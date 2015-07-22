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

import org.gradoop.model.helper.Order;
import org.gradoop.model.helper.Predicate;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.impl.EPGraph;
import org.gradoop.model.impl.EPGraphCollection;
import org.gradoop.model.impl.operators.Aggregation;
import org.gradoop.model.impl.operators.Combination;
import org.gradoop.model.impl.operators.Projection;
import org.gradoop.model.impl.operators.Summarization;
import org.gradoop.model.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.store.EPGraphStore;
import org.mockito.Mockito;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class WorkflowTest {

  public void summarizedCommunities() throws Exception {
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
    EPGraph knowsGraph = friendships.reduce(new Combination());

    // apply label propagation
    EPGraphCollection communities =
      knowsGraph.callForCollection(new LP("communityID"));
    // and build one graph
    knowsGraph = communities.reduce(new Combination());

    Summarization summarization =
      new Summarization.SummarizationBuilder("city", true)
        .edgeGroupingKey("since").build();

    knowsGraph.callForGraph(summarization);

    // summarize communities
    knowsGraph.summarize("city", "since");
  }

  public void topRevenueBusinessProcess() throws Exception {
    EPGraphStore db = Mockito.mock(EPGraphStore.class);

    // read full graph from database
    EPGraph dbGraph = db.getDatabaseGraph();

    // extract business process instances
    EPGraphCollection btgs = dbGraph.callForCollection(new BTG());

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
    final UnaryFunction<EPGraph, Double> aggregateFunc =
      new UnaryFunction<EPGraph, Double>() {
        @Override
        public Double execute(EPGraph entity) {
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
      btgs.select(predicate).apply(new Aggregation<>("revenue", aggregateFunc));

    // sort graphs by revenue and return top 100
    EPGraphCollection topBTGs =
      invBtgs.sortBy("revenue", Order.DESCENDING).top(100);

    // compute overlap to find master store objects (e.g. Employee)
    EPGraph topOverlap = topBTGs.reduce(new Combination());
  }

  public void clusterCharacteristicPatterns() throws Exception {
    EPGraphStore db = Mockito.mock(EPGraphStore.class);

    // generate base collection
    EPGraphCollection btgs = db.getDatabaseGraph()
      .callForCollection(new UnaryGraphToCollectionOperator() {
        @Override
        public EPGraphCollection execute(EPGraph graph) {
          // TODO execute BTG Computation
          throw new NotImplementedException();
        }

        @Override
        public String getName() {
          return "BTGComputation";
        }
      });

    // define aggregate function (profit per graph)
    final UnaryFunction<EPGraph, Double> aggFunc =
      new UnaryFunction<EPGraph, Double>() {
        @Override
        public Double execute(EPGraph entity) {
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
    btgs = btgs.apply(new Aggregation<>("profit", aggFunc));

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
    btgs = btgs.apply(new Projection(vertexFunc, edgeFunc));

    // select profit and loss clusters
    EPGraphCollection profitBtgs = btgs.filter(new Predicate<EPGraphData>() {
      @Override
      public boolean filter(EPGraphData entity) {
        return (Double) entity.getProperty("result") >= 0;
      }
    });
    EPGraphCollection lossBtgs = btgs.difference(profitBtgs);

    EPGraphCollection profitFreqPats =
      profitBtgs.callForCollection(new FSM(0.7f));
    EPGraphCollection lossFreqPats = lossBtgs.callForCollection(new FSM(0.7f));

    // determine cluster characteristic patterns
    EPGraphCollection trivialPats = profitFreqPats.intersect(lossFreqPats);
    EPGraphCollection profitCharPatterns =
      profitFreqPats.difference(trivialPats);
    EPGraphCollection lossCharPatterns = lossFreqPats.difference(trivialPats);
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
    public EPGraphCollection execute(EPGraphCollection collection) {
      throw new NotImplementedException();
    }
  }

  private static class LP implements UnaryGraphToCollectionOperator {

    private final String propertyKey;

    public LP(String propertyKey) {

      this.propertyKey = propertyKey;
    }

    @Override
    public EPGraphCollection execute(EPGraph graph) {
      throw new NotImplementedException();
    }

    @Override
    public String getName() {
      return "LabelPropagation";
    }
  }

  private static class BTG implements UnaryGraphToCollectionOperator {

    @Override
    public EPGraphCollection execute(EPGraph graph) {
      // TODO execute BTG Computation
      throw new NotImplementedException();
    }

    @Override
    public String getName() {
      return "BTGComputation";
    }
  }
}
