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

package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;

import java.util.List;
import java.util.Random;

/**
 * Creates a person vertex.
 */
public abstract class BusinessRelation extends Person {

  /**
   * List of possible companies.
   */
  private List<String> companies;
  /**
   * List of possible holdings.
   */
  private List<String> holdings;

  /**
   * Valued constructor.
   *
   * @param vertexFactory    EPGM vertex factory
   * @param foodBrokerConfig FoodBroker configuration
   */
  public BusinessRelation(VertexFactory vertexFactory, FoodBrokerConfig foodBrokerConfig) {
    super(vertexFactory, foodBrokerConfig);
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // load broadcasted lists
    companies = getRuntimeContext().getBroadcastVariable(Constants.COMPANIES_BC);
    holdings = getRuntimeContext().getBroadcastVariable(Constants.HOLDINGS_BC);
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    Vertex vertex = super.map(seed);

    // set rnd company
    Random rand = new Random();
    int companyNumber = rand.nextInt(getFoodBrokerConfig().getCompanyCount());
    String company = companies.get(companyNumber);
    String holding = holdings.get(companyNumber % getFoodBrokerConfig().getHoldingCount());
    int branchNumber = rand.nextInt(
      (getFoodBrokerConfig().getBranchMaxAmount() -
        getFoodBrokerConfig().getBranchMinAmount()) +
      1) + getFoodBrokerConfig().getBranchMinAmount();
    vertex.setProperty(Constants.BRANCHNUMBER_KEY, branchNumber);
    vertex.setProperty(Constants.COMPANY_KEY, company);
    vertex.setProperty(Constants.HOLDING_KEY, holding);

    return vertex;
  }
}
