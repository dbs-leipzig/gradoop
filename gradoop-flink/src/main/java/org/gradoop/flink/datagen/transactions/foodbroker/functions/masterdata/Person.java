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
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;

import java.util.List;
import java.util.Random;

/**
 * Creates a person vertex.
 */
public abstract class Person extends MasterData {
  /**
   * List of possible cities.
   */
  private List<String> cities;
  /**
   * List of possible companies.
   */
  private List<String> companies;
  /**
   * List of possible holdings.
   */
  private List<String> holdings;
  /**
   * Amount of pissible cities.
   */
  private Integer cityCount;
  /**
   * EPGM vertex factory.
   */
  private final VertexFactory vertexFactory;
  /**
   * FoodBroker configuration.
   */
  private FoodBrokerConfig foodBrokerConfig;

  /**
   * Valued constructor.
   *
   * @param vertexFactory EPGM vertex factory
   */
  public Person(VertexFactory vertexFactory, FoodBrokerConfig foodBrokerConfig) {
    this.vertexFactory = vertexFactory;
    this.foodBrokerConfig = foodBrokerConfig;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    //load broadcasted lists
    cities = getRuntimeContext().getBroadcastVariable(Constants.CITIES_BC);
    companies = getRuntimeContext().getBroadcastVariable(Constants.COMPANIES_BC);
    holdings = getRuntimeContext().getBroadcastVariable(Constants.HOLDINGS_BC);
    //get the size
    cityCount = cities.size();
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    //create standard properties from acronym and seed
    Properties properties = createDefaultProperties(seed, getAcronym());
    Random random = new Random();
    //set rnd location
    String[] location = cities.get(random.nextInt(cityCount)).split("-");
    properties.set(Constants.CITY_KEY, location[0]);
    properties.set(Constants.STATE_KEY, location[1]);
    properties.set(Constants.COUNTRY_KEY, location[2]);

    //set rnd company
    Random rand = new Random();
    int companyNumber = rand.nextInt(foodBrokerConfig.getCompanyCount());
    String company = companies.get(companyNumber);
    String holding = holdings.get(companyNumber % foodBrokerConfig.getHoldingCount());
    int branchNumber = rand.nextInt(
      (foodBrokerConfig.getBranchMaxAmount() - foodBrokerConfig.getBranchMinAmount()) + 1) +
      foodBrokerConfig.getBranchMinAmount();
    properties.set(Constants.BRANCHNUMBER_KEY, branchNumber);
    properties.set(Constants.COMPANY_KEY, company);
    properties.set(Constants.HOLDING_KEY, holding);

    //update quality
    Float quality = properties.get(Constants.QUALITY_KEY).getFloat();
    Double assistantRatio = foodBrokerConfig.getMasterDataTypeAssistantRatio(getClassName());
    Double normalRatio = foodBrokerConfig.getMasterDataTypeNormalRatio(getClassName());
    Double supervisorRatio = foodBrokerConfig.getMasterDataTypeSupervisorRatio(getClassName());
    Double rnd = rand.nextDouble();
    if (rnd <= assistantRatio) {
      quality += quality * Constants.TYPE_ASSISTANT_INFLUENCE;
      properties.set(Constants.TYPE_KEY, Constants.TYPE_ASSISTANT);
    } else if (rnd >= assistantRatio + normalRatio) {
      quality += quality * Constants.TYPE_SUPERVISOR_INFLUENCE;
      if (quality > 1f) {
        quality = 1f;
      }
      properties.set(Constants.TYPE_KEY, Constants.TYPE_SUPERVISOR);
    } else {
      properties.set(Constants.TYPE_KEY, Constants.TYPE_NORMAL);
    }
    properties.set(Constants.QUALITY_KEY, quality);

    return vertexFactory.createVertex(getClassName(), properties);
  }
}
