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
package org.gradoop.benchmark.sampling;

import org.gradoop.flink.model.impl.operators.sampling.RandomVertexSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexNeighborhoodSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexEdgeSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomNonUniformVertexSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomLimitedDegreeVertexSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomEdgeSampling;
import org.gradoop.flink.model.impl.operators.sampling.PageRankSampling;
import org.gradoop.flink.model.impl.operators.sampling.SamplingAlgorithm;

/**
 * A builder class used to instantiate a specific sampling algorithm.
 */
class SamplingBuilder {

  /**
   * Enum of available sampling algorithms. Enum is constructed with meta information about a given
   * sampling algorithm like its name.
   */
  private enum Algorithm {
    /** PageRankSampling enum constant */
    PAGE_RANK_SAMPLING(PageRankSampling.class.getSimpleName()),
    /** RandomEdgeSampling enum constant */
    RANDOM_EDGE_SAMPLING(RandomEdgeSampling.class.getSimpleName()),
    /** RandomLimitedDegreeVertexSampling enum constant */
    RANDOM_LIMITED_DEGREE_VERTEX_SAMPLING(RandomLimitedDegreeVertexSampling.class.getSimpleName()),
    /** RandomNonUniformVertexSampling enum constant */
    RANDOM_NON_UNIFORM_VERTEX_SAMPLING(RandomNonUniformVertexSampling.class.getSimpleName()),
    /** RandomVertexEdgeSampling enum constant */
    RANDOM_VERTEX_EDGE_SAMPLING(RandomVertexEdgeSampling.class.getSimpleName()),
    /** RandomVertexNeighborhoodSampling enum constant */
    RANDOM_VERTEX_NEIGHBORHOOD_SAMPLING(RandomVertexNeighborhoodSampling.class.getSimpleName()),
    /** RandomVertexSampling enum constant */
    RANDOM_VERTEX_SAMPLING(RandomVertexSampling.class.getSimpleName());

    /** Property denoting the simple classname of a sampling algorithm */
    private final String name;

    /**
     * Enum constructor.
     *
     * @param name simplified class name
     */
    Algorithm(String name) {
      this.name = name;
    }
  }

  /**
   * Tries to instantiate a SamplingAlgorithm defined by the enum Algorithm. Maps the
   * value of SELECTED_ALGORITHM and the length of CONSTRUCTOR_PARAMS to a specific constructor.
   * Constructors that take non-primitive data types as input are not provided.
   * If the value of SELECTED_ALGORITHM does not match any ordinal of the enum, or the length of
   * CONSTRUCTOR_PARAMS does not match any signature of a specified SamplingAlgorithm, an exception
   * is thrown.
   *
   * @param ordinal Integer indicating which sampling algorithm to instantiate.
   *                Value must correspond to to an Algorithm enum ordinal.
   * @param constructorParams Array containing parameters used to construct a sampling algorithm.
   * @return SamplingAlgorithm specified by const SELECTED_ALGORITHM.
   */
  static SamplingAlgorithm buildSelectedSamplingAlgorithm(int ordinal, String[] constructorParams) {

    performSanityCheck(ordinal);

    switch (Algorithm.values()[ordinal]) {

    case PAGE_RANK_SAMPLING:
      if (constructorParams.length == 5) {
        return new PageRankSampling(
          Double.parseDouble(constructorParams[0]),
          Integer.parseInt(constructorParams[1]),
          Double.parseDouble(constructorParams[2]),
          Boolean.parseBoolean(constructorParams[3]),
          Boolean.parseBoolean(constructorParams[4]));
      } else {
        throw createInstantiationException(Algorithm.PAGE_RANK_SAMPLING.name, new String[]{"5"},
          constructorParams.length);
      }

    case RANDOM_EDGE_SAMPLING:
      if (constructorParams.length == 1) {
        return new RandomEdgeSampling(Float.parseFloat(constructorParams[0]));
      } else if (constructorParams.length == 2) {
        return new RandomEdgeSampling(Float.parseFloat(constructorParams[0]),
          Long.parseLong(constructorParams[1]));
      } else {
        throw createInstantiationException(Algorithm.RANDOM_EDGE_SAMPLING.name,
          new String[]{"1", "2"}, constructorParams.length);
      }

    case RANDOM_LIMITED_DEGREE_VERTEX_SAMPLING:
      if (constructorParams.length == 1) {
        return new RandomLimitedDegreeVertexSampling(Float.parseFloat(constructorParams[0]));
      } else if (constructorParams.length == 2) {
        return new RandomLimitedDegreeVertexSampling(Float.parseFloat(constructorParams[0]),
          Long.parseLong(constructorParams[1]));
      } else {
        throw createInstantiationException(Algorithm.RANDOM_LIMITED_DEGREE_VERTEX_SAMPLING.name,
          new String[]{"1", "2"}, constructorParams.length);
      }

    case RANDOM_NON_UNIFORM_VERTEX_SAMPLING:
      if (constructorParams.length == 1) {
        return new RandomNonUniformVertexSampling(Float.parseFloat(constructorParams[0]));
      } else if (constructorParams.length == 2) {
        return new RandomNonUniformVertexSampling(Float.parseFloat(constructorParams[0]),
          Long.parseLong(constructorParams[1]));
      } else {
        throw createInstantiationException(Algorithm.RANDOM_NON_UNIFORM_VERTEX_SAMPLING.name,
          new String[]{"1", "2"}, constructorParams.length);
      }

    case RANDOM_VERTEX_EDGE_SAMPLING:
      if (constructorParams.length == 1) {
        return new RandomVertexEdgeSampling(Float.parseFloat(constructorParams[0]));
      } else if (constructorParams.length == 2) {
        return new RandomVertexEdgeSampling(Float.parseFloat(constructorParams[0]),
          Float.parseFloat(constructorParams[1]));
      } else {
        throw createInstantiationException(Algorithm.RANDOM_VERTEX_EDGE_SAMPLING.name,
          new String[]{"1", "2"}, constructorParams.length);
      }

    case RANDOM_VERTEX_NEIGHBORHOOD_SAMPLING:
      if (constructorParams.length == 1) {
        return new RandomVertexNeighborhoodSampling(Float.parseFloat(constructorParams[0]));
      } else if (constructorParams.length == 2) {
        return new RandomVertexNeighborhoodSampling(Float.parseFloat(constructorParams[0]),
          Long.parseLong(constructorParams[1]));
      } else {
        throw createInstantiationException(Algorithm.RANDOM_VERTEX_NEIGHBORHOOD_SAMPLING.name,
          new String[]{"1", "2"}, constructorParams.length);
      }

    case RANDOM_VERTEX_SAMPLING:
      if (constructorParams.length == 1) {
        return new RandomVertexSampling(Float.parseFloat(constructorParams[0]));
      } else if (constructorParams.length == 2) {
        return new RandomVertexSampling(Float.parseFloat(constructorParams[0]),
          Long.parseLong(constructorParams[1]));
      } else {
        throw createInstantiationException(Algorithm.RANDOM_VERTEX_SAMPLING.name,
          new String[]{"1", "2"}, constructorParams.length);
      }

    default:
      throw  new IllegalArgumentException(
        "Something went wrong. Please select an other sampling algorithm.");
    }
  }


  /**
   * Helper function to create string that can be used to display available associations between the
   * ordinals of the enum Algorithm and the sampling algorithms themselves.
   *
   * @return String providing association information.
   */
  private static String getOrdinalAlgorithmAssociation() {
    StringBuilder result = new StringBuilder("Available Algorithms:\n");
    for (Algorithm alg : Algorithm.values()) {
      result.append(String.format("%d ---> %s%n", alg.ordinal(), alg.name));
    }
    return result.toString();
  }

  /**
   * Helper function used to create an exception that is specific to function
   * buildSelectedSamplingAlgorithm().
   *
   * @param name Name of the sampling class.
   * @param amountArgs Array of strings containing all possible argument counts for a given sampling
   *                  class.
   * @param amountProvidedArgs Integer indicating the amount of provided constructor args.
   * @return IllegalArgumentException
   */
  private static IllegalArgumentException createInstantiationException(
    String name,
    String[] amountArgs,
    int amountProvidedArgs
  ) {
    return new IllegalArgumentException(String.format(
      "Constructor of %s requires %s arguments. %d provided.",
      name, String.join(" or ", amountArgs), amountProvidedArgs
    ));
  }

  /**
   * Checks if the provided value corresponds to an enum ordinal.
   *
   * @param ordinal Integer referring to an ordinal.
   */
  private static void performSanityCheck(int ordinal) {
    if (ordinal < 0 || ordinal > Algorithm.values().length - 1) {
      throw new IllegalArgumentException(String.format("No sampling algorithm associated with %d." +
          " Please provide an integer in the range 0 - %d%n%s",
        ordinal,
        Algorithm.values().length - 1,
        getOrdinalAlgorithmAssociation()));
    }
  }
}
