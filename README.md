### Gradoop : Graph Analytics Framework on Apache Hadoop
***

#### Setup development environment

##### Requirements

* Maven 3
* JDK 7 (Oracle or OpenJDK)

##### Setup giraph

Unfortunately, Giraph is not available in the official Apache Maven 
repositories, so we have to build it on our own.

* check out Giraph from dbs-leipzig/giraph

    `git clone -b trunk https://github.com/dbs-leipzig/giraph.git`
    
* install to local maven repository

    `cd giraph`
    
    `mvn -Phadoop_1 clean install -DskipTests`
    
##### Setup gradoop

* check out gradoop

    `git clone -b master https://github.com/s1ck/gradoop.git`
    
* build and run tests

    `cd gradoop`
    
    `mvn clean install`

##### Code style for IntelliJ IDEA

* copy codestyle from dev-support to your local IDEA config folder

    `cp dev-support/gradoop-idea-codestyle.xml ~/<your IDEA folder>/config/codeStyles`
    
* restart IDEA
* `File -> Settings -> Code Style -> Java -> Scheme -> "Gradoop"`
    





