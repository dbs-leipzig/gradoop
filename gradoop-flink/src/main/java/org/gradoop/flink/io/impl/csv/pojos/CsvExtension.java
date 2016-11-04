package org.gradoop.flink.io.impl.csv.pojos;

/**
 * Created by stephan on 04.11.16.
 */
public class CsvExtension extends Csv {
    private String datasourceName;

    private String domainName;

    public String getDatasourceName() {
        return datasourceName;
    }

    public void setDatasourceName(String datasourceName) {
        this.datasourceName = datasourceName;
    }

    public String getDomainName() {
        return domainName;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }
}