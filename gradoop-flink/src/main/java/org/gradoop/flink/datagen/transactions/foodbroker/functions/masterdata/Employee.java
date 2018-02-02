/**
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
 * Creates a employee vertex.
 */
public class Employee extends Person {
  /**
   * List of possible first female names.
   */
  private List<String> firstNamesFemale;
  /**
   * List of possible first male names.
   */
  private List<String> firstNamesMale;
  /**
   * List of possible last names.
   */
  private List<String> lastNames;
  /**
   * Amount of possible female first names.
   */
  private Integer firstNameCountFemale;
  /**
   * Amount of possible male first names.
   */
  private Integer firstNameCountMale;
  /**
   * Amount of possible last names.
   */
  private Integer lastNameCount;

  /**
   * Valued constructor.
   *
   * @param vertexFactory EPGM vertex factory.
   * @param foodBrokerConfig FoodBroker configuration.
   */
  public Employee(VertexFactory vertexFactory, FoodBrokerConfig foodBrokerConfig) {
    super(vertexFactory, foodBrokerConfig);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    //load broadcast maps
    firstNamesFemale = getRuntimeContext().getBroadcastVariable(Constants.FIRST_NAMES_FEMALE_BC);
    firstNamesMale = getRuntimeContext().getBroadcastVariable(Constants.FIRST_NAMES_MALE_BC);
    lastNames = getRuntimeContext().getBroadcastVariable(Constants.LAST_NAMES_BC);
    //get their sizes.
    firstNameCountFemale = firstNamesFemale.size();
    firstNameCountMale = firstNamesMale.size();
    lastNameCount = lastNames.size();
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    Vertex vertex = super.map(seed);
    Random random = new Random();
    //set rnd name and gender
    String gender;
    String name;
    // separate between male and female and load the corresponding names
    if (seed.getNumber() % 2 == 0) {
      gender = "f";
      name = firstNamesFemale.get(random.nextInt(firstNameCountFemale)) +
        " " + lastNames.get(random.nextInt(lastNameCount));
    } else {
      gender = "m";
      name = firstNamesMale.get(random.nextInt(firstNameCountMale)) +
        " " + lastNames.get(random.nextInt(lastNameCount));
    }
    vertex.setProperty(Constants.NAME_KEY, name);
    vertex.setProperty(Constants.GENDER_KEY, gender);

    //update quality and set type
    Float quality = vertex.getPropertyValue(Constants.QUALITY_KEY).getFloat();
    Double assistantRatio = getFoodBrokerConfig().getMasterDataTypeAssistantRatio(getClassName());
    Double normalRatio = getFoodBrokerConfig().getMasterDataTypeNormalRatio(getClassName());
    Double rnd = random.nextDouble();
    if (rnd <= assistantRatio) {
      quality *= getFoodBrokerConfig().getMasterDataTypeAssistantInfluence();
      vertex.setProperty(Constants.EMPLOYEE_TYPE_KEY, Constants.EMPLOYEE_TYPE_ASSISTANT);
    } else if (rnd >= assistantRatio + normalRatio) {
      quality *= getFoodBrokerConfig().getMasterDataTypeSupervisorInfluence();
      if (quality > 1f) {
        quality = 1f;
      }
      vertex.setProperty(Constants.EMPLOYEE_TYPE_KEY, Constants.EMPLOYEE_TYPE_SUPERVISOR);
    } else {
      vertex.setProperty(Constants.EMPLOYEE_TYPE_KEY, Constants.EMPLOYEE_TYPE_NORMAL);
    }
    vertex.setProperty(Constants.QUALITY_KEY, quality);

    return vertex;
  }

  @Override
  public String getAcronym() {
    return Constants.EMPLOYEE_ACRONYM;
  }

  @Override
  public String getClassName() {
    return Constants.EMPLOYEE_VERTEX_LABEL;
  }
}
