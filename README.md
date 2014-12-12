### Gradoop : Graph Analytics Framework on Apache Hadoop
***

#### Setup development environment

##### Requirements

* Maven 3
* JDK 7 (Oracle or OpenJDK)

##### Setup Gradoop

* clone Gradoop to your local file system

    `git clone https://github.com/s1ck/gradoop.git`
    
* build and run tests

    `cd gradoop`
    
    `mvn clean install`

##### Code style for IntelliJ IDEA

* copy codestyle from dev-support to your local IDEA config folder

    `cp dev-support/gradoop-idea-codestyle.xml ~/<your IDEA folder>/config/codeStyles`
    
* restart IDEA
* `File -> Settings -> Code Style -> Java -> Scheme -> "Gradoop"`

##### Troubleshooting

* Exception while running test org.apache.giraph.io.hbase
.TestHBaseRootMarkerVertextFormat (incorrect permissions, see
http://stackoverflow.com/questions/17625938/hbase-minidfscluster-java-fails
-in-certain-environments for details)

    `umask 022`

* Ubuntu + Giraph hostname problems. To avoid hostname issues comment the
following line in /etc/hosts

    `127.0.1.1   <your-host-name>`
    





