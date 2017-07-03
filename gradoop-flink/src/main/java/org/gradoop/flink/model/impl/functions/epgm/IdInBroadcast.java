package org.gradoop.flink.model.impl.functions.epgm;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Collection;

/**
 * Filters a dataset of EPGM elements to those whose id is contained in an id dataset.
 *
 * @param <EL> element type
 */
public class IdInBroadcast<EL extends EPGMElement> extends RichFilterFunction<EL> {

  /**
   * broadcast id set name
   */
  public static final String IDS = "Ids";

  /**
   * graph ids
   */
  protected Collection<GradoopId> ids;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    ids = getRuntimeContext().getBroadcastVariable(IDS);
    ids = Sets.newHashSet(ids);
  }

  @Override
  public boolean filter(EL identifiable) throws Exception {
    return ids.contains(identifiable.getId());
  }
}
