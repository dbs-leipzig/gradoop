package org.gradoop.flink.representation.common.elementdata;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.representation.transactional.adjacencylist.IdLabel;


public class IdLabelFactory implements ElementDataFactory<IdLabel> {

  @Override
  public IdLabel createValue(Element element) {
    return new IdLabel(element.getId(), element.getLabel());
  }
}
