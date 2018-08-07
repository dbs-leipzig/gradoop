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
package org.gradoop.storage.impl.accumulo.predicate.filter.api;

import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.storage.common.predicate.filter.api.ElementFilter;
import org.gradoop.storage.impl.accumulo.iterator.tserver.GradoopEdgeIterator;
import org.gradoop.storage.impl.accumulo.iterator.tserver.GradoopGraphHeadIterator;
import org.gradoop.storage.impl.accumulo.iterator.tserver.GradoopVertexIterator;
import org.gradoop.storage.impl.accumulo.predicate.filter.calculate.And;
import org.gradoop.storage.impl.accumulo.predicate.filter.calculate.Not;
import org.gradoop.storage.impl.accumulo.predicate.filter.calculate.Or;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.function.Predicate;

/**
 * Accumulo Element Filter
 *
 * @param <T> epgm element type
 * @see GradoopEdgeIterator
 * @see GradoopGraphHeadIterator
 * @see GradoopVertexIterator
 */
public interface AccumuloElementFilter<T extends EPGMElement>
  extends Predicate<T>, ElementFilter<AccumuloElementFilter<T>>, Serializable {

  /**
   * anti-serialize reducer from base64 encoded string
   * this action will be execute by tserver
   *
   * @param encoded encoded string
   * @param <T> filter element type
   * @return filter instance
   */
  @Nonnull
  static <T extends EPGMElement> AccumuloElementFilter<T> decode(String encoded) {
    byte[] content = Base64.getDecoder().decode(encoded);
    try (
      ByteArrayInputStream arr = new ByteArrayInputStream(content);
      ObjectInput in = new ObjectInputStream(arr)) {
      //noinspection unchecked
      return (AccumuloElementFilter<T>) in.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * serialize reducer as base64 encoded string
   * this action will be execute by client
   *
   * @return encoded string
   */
  default String encode() {
    try (
      ByteArrayOutputStream arr = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(arr)) {
      out.writeObject(this);
      return Base64.getEncoder().encodeToString(arr.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * disjunctive operator
   *
   * @param another another reduce filter
   * @return conjunctive logic filter
   */
  @Nonnull
  default AccumuloElementFilter<T> or(@Nonnull AccumuloElementFilter<T> another) {
    return Or.create(this, another);
  }

  /**
   * conjunctive operator
   *
   * @param another another reduce filter
   * @return conjunctive logic filter
   */
  @Nonnull
  default AccumuloElementFilter<T> and(@Nonnull AccumuloElementFilter<T> another) {
    return And.create(this, another);
  }

  /**
   * negative operator
   * @return negative logic for current filter
   */
  @Nonnull
  default AccumuloElementFilter<T> negate() {
    return Not.of(this);
  }

}
