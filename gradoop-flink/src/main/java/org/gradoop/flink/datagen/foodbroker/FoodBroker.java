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
package org.gradoop.flink.datagen.foodbroker;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.foodbroker.functions.GraphIdsMapFromTuple;
import org.gradoop.flink.datagen.foodbroker.functions.GraphIdsTupleFromEdge;
import org.gradoop.flink.datagen.foodbroker.functions.SetMasterDataGraphIds;
import org.gradoop.flink.datagen.foodbroker.process.ComplaintHandling;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.process.Brokerage;
import org.gradoop.flink.datagen.foodbroker.masterdata.*;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.GraphTransactionTriple;
import org.gradoop.flink.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.flink.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.model.api.operators.CollectionGenerator;

import java.util.Map;

/**
 * Generates a GraphCollection containing a foodbrokerage and a complaint
 * handling process.
 */
public class FoodBroker implements CollectionGenerator {
  /**
   * Execution environment
   */
  protected final ExecutionEnvironment env;
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig gradoopFlinkConfig;
  /**
   * Foodbroker configuration
   */
  private final FoodBrokerConfig foodBrokerConfig;

  /**
   * Valued constructor.
   *
   * @param env execution environment
   * @param gradoopFlinkConfig Gradoop Flink configuration
   * @param foodBrokerConfig Foodbroker configuration
   */
  public FoodBroker(ExecutionEnvironment env,
    GradoopFlinkConfig gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig) {

    this.env = env;
    this.gradoopFlinkConfig = gradoopFlinkConfig;
    this.foodBrokerConfig = foodBrokerConfig;
  }

  @Override
  public GraphCollection execute() {

    // used for type hinting when loading graph head data
    TypeInformation<GraphHead> graphHeadTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getGraphHeadFactory().getType());
    // used for type hinting when loading vertex data
    TypeInformation<Vertex> vertexTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation<Edge> edgeTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getEdgeFactory().getType());

    DataSet<Vertex> customers =
      new CustomerGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<Vertex> vendors =
      new VendorGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<Vertex> logistics =
      new LogisticsGenerator(gradoopFlinkConfig, foodBrokerConfig)
        .generate();

    final DataSet<Vertex> employees =
      new EmployeeGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<Vertex> products =
      new ProductGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<Long> caseSeeds = env.generateSequence(1, foodBrokerConfig
      .getCaseCount());

    Brokerage brokerage = new Brokerage(foodBrokerConfig, gradoopFlinkConfig,
      customers, vendors, logistics, employees, products, caseSeeds);
    brokerage.execute();

    ComplaintHandling complaintHandling = new ComplaintHandling(
      foodBrokerConfig, gradoopFlinkConfig, customers, vendors, logistics,
        employees, products, caseSeeds, brokerage.getTuple());
    complaintHandling.execute();

    DataSet<Vertex> transactionalVertices = brokerage.getTransactions()
      .union(complaintHandling.getTransactions())
      .map(new GraphTransactionTriple())
      .flatMap(new TransactionVertices())
      .returns(vertexTypeInfo);

    DataSet<Edge> transactionalEdges = brokerage.getTransactions()
      .union(complaintHandling.getTransactions())
      .map(new GraphTransactionTriple())
      .flatMap(new TransactionEdges())
      .returns(edgeTypeInfo);

    DataSet<GraphHead> graphHeads = brokerage.getTransactions()
      .union(complaintHandling.getTransactions())
      .map(new GraphTransactionTriple())
      .map(new TransactionGraphHead())
      .returns(graphHeadTypeInfo);

    DataSet<Map<GradoopId, GradoopIdSet>> graphIds = transactionalEdges
      .map(new GraphIdsTupleFromEdge())
      .reduceGroup(new GraphIdsMapFromTuple());

    DataSet<Vertex> masterData = customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products)
      .union(complaintHandling.getNewMasterData())
      .map(new SetMasterDataGraphIds())
      .withBroadcastSet(graphIds, "graphIds");

    DataSet<Vertex> vertices = masterData
      .union(transactionalVertices);

    return GraphCollection.fromDataSets(graphHeads, vertices,
      transactionalEdges, gradoopFlinkConfig);
  }

  @Override
  public String getName() {
    return "FoodBroker Data Generator";
  }
}
