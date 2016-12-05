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

package org.gradoop.flink.datagen.foodbroker.functions.masterdata;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.foodbroker.tuples.MasterDataSeed;

import java.util.List;
import java.util.Random;

/**
 * Creates a employee vertex.
 */
public class Employee extends RichMapFunction<MasterDataSeed, Vertex> {
  /**
   * Class name of the vertex.
   */
  public static final String CLASS_NAME = "Employee";
  /**
   * Broadcast variable for male employees first name.
   */
  public static final String FIRST_NAMES_MALE_BC = "firstNamesMale";
  /**
   * Broadcast variable for female employees first name.
   */
  public static final String FIRST_NAMES_FEMALE_BC = "firstNamesFemale";
  /**
   * Broadcast variable for employees last name.
   */
  public static final String LAST_NAMES_BC = "nouns";
  /**
   * Broadcast variable for employees cities.
   */
  public static final String CITIES_BC = "cities";
  /**
   * Acronym for employee.
   */
  private static final String ACRONYM = "EMP";
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
   * List of possible cities.
   */
  private List<String> cities;
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
   * Amount of possible cities.
   */
  private Integer cityCount;
  /**
   * EPGM vertex factory.
   */
  private final VertexFactory vertexFactory;

  /**
   * Valued constructor.
   *
   * @param vertexFactory EPGM vertex factory.
   */
  public Employee(VertexFactory vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    //load broadcast maps
    firstNamesFemale = getRuntimeContext()
      .getBroadcastVariable(FIRST_NAMES_FEMALE_BC);
    firstNamesMale = getRuntimeContext()
      .getBroadcastVariable(FIRST_NAMES_MALE_BC);
    lastNames = getRuntimeContext()
      .getBroadcastVariable(LAST_NAMES_BC);
    cities = getRuntimeContext()
      .getBroadcastVariable(CITIES_BC);
    //get their sizes.
    firstNameCountFemale = firstNamesFemale.size();
    firstNameCountMale = firstNamesMale.size();
    lastNameCount = lastNames.size();
    cityCount = cities.size();
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    //create standard properties from acronym and seed
    Properties properties = MasterData.createDefaultProperties(seed, ACRONYM);
    Random random = new Random();
    //set rnd city, name and gender
    properties.set("city", cities.get(random.nextInt(cityCount)));

    String gender;
    String name;

    if (seed.getNumber() % 2 == 0) {
      gender = "f";
      name = firstNamesFemale.get(random.nextInt(firstNameCountFemale)) +
        " " + lastNames.get(random.nextInt(lastNameCount));
    } else {
      gender = "m";
      name = firstNamesMale.get(random.nextInt(firstNameCountMale)) +
        " " + lastNames.get(random.nextInt(lastNameCount));
    }
    properties.set("name", name);
    properties.set("gender", gender);
    return vertexFactory.createVertex(Employee.CLASS_NAME, properties);
  }
}
