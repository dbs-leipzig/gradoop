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
package org.gradoop.storage.common.predicate.query;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.storage.common.predicate.filter.api.ElementFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Element Query Formula
 *
 * A element query contains
 *  - id range set (which define the query id range of result element)
 *  - element filter expression (which define should a result element be return from server)
 *
 * @see Query#elements()
 * @param <FilterImpl> filter implement type
 */
public class ElementQuery<FilterImpl extends ElementFilter> implements Serializable {

  /**
   * gradoop id range
   */
  private final GradoopIdSet ranges;

  /**
   * filter predicate
   */
  private final FilterImpl filter;

  /**
   * element query with range constructor
   * @param range element query range
   * @param filter filter definition
   */
  private ElementQuery(
    @Nullable GradoopIdSet range,
    @Nullable FilterImpl filter
  ) {
    this.ranges = range;
    this.filter = filter;
  }

  /**
   * get filter predicate
   *
   * @return predicate implement
   */
  @Nullable
  public FilterImpl getFilterPredicate() {
    return this.filter;
  }

  /**
   * get query range by gradoop id
   *
   * @return gradoop id set
   */
  @Nullable
  public GradoopIdSet getQueryRanges() {
    return ranges;
  }

  @Override
  public String toString() {
    return String.format("QUERY ELEMENT FROM %1$s %2$s",
      ranges == null ? "ALL" : ranges,
      filter == null ? "" : ("WHERE " + filter));
  }

  /**
   * [Builder] - (set range) -> BuilderWithRange -(set filter) -> ElementQuery
   */
  public static class Builder {

    /**
     * query all range
     *
     * @return element query with all range
     */
    @Nonnull
    public BuilderWithRange fromAll() {
      return new BuilderWithRange(null);
    }

    /**
     * query element in a certain id range set
     *
     * @param gradoopIds query range
     * @return element query with a certain range
     */
    @Nonnull
    public BuilderWithRange fromSets(@Nonnull Collection<String> gradoopIds) {
      return new BuilderWithRange(GradoopIdSet.fromExisting(gradoopIds.stream()
        .map(GradoopId::fromString)
        .collect(Collectors.toSet())));
    }

    /**
     * query element in a certain id range set
     *
     * @param gradoopIds query range
     * @return element query with a certain range
     */
    @Nonnull
    public BuilderWithRange fromSets(@Nonnull GradoopIdSet gradoopIds) {
      return new BuilderWithRange(gradoopIds);
    }

    /**
     * query element in a certain id range set
     *
     * @param gradoopIds query range
     * @return element query with a certain range
     */
    @Nonnull
    public BuilderWithRange fromSets(@Nonnull GradoopId... gradoopIds) {
      return new BuilderWithRange(GradoopIdSet.fromExisting(gradoopIds));
    }

  }

  /**
   * Builder - (set range) -> [BuilderWithRange] -(set filter) -> ElementQuery
   */
  public static class BuilderWithRange {

    /**
     * gradoop id range
     */
    private final GradoopIdSet range;

    /**
     * element query with range constructor
     * @param range element query range
     */
    BuilderWithRange(@Nullable GradoopIdSet range) {
      this.range = range;
    }

    /**
     * element query with range and without filter
     *
     * @param <FilterImpl> element filter type, a dummy definition
     * @return element query with range and without filter
     */
    @Nonnull
    public <FilterImpl extends ElementFilter> ElementQuery<FilterImpl> noFilter() {
      return new ElementQuery<>(range, null);
    }

    /**
     * element query with range and filter
     *
     * @param filter element query with range and filter
     * @param <FilterImpl> filter implement
     * @return element query with range and filter
     */
    @Nonnull
    public <FilterImpl extends ElementFilter> ElementQuery<FilterImpl> where(
      @Nonnull FilterImpl filter
    ) {
      return new ElementQuery<>(range, filter);
    }

  }

}
