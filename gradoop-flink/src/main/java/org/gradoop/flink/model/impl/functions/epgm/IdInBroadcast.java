package org.gradoop.flink.model.impl.functions.epgm;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.api.entities.EPGMIdentifiable;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Collection;

public class IdInBroadcast<EI extends EPGMIdentifiable> extends RichFilterFunction<EI> {

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
  public boolean filter(EI identifiable) throws Exception {
    return ids.contains(identifiable.getId());
  }
}
