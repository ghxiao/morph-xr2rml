## What is it?
Morph-xR2RML is an implementation of the xR2RML mapping language, to describe mappings from relational or non relational databases to RDF.
The current implementation supports the xR2RML language for relational databases (MySQL, PostgreSQL, MonetDB), and the MongoDB NoSQL document store.

The xR2RML mapping language (https://hal.archives-ouvertes.fr/hal-01066663 V3) is an extension of R2RML (http://www.w3.org/TR/r2rml/) and RML (http://semweb.mmlab.be/rml/spec.html).

Morph-xR2RML was developed by the I3S laboratory (http://www.i3s.unice.fr/) as an extension of the Morph-RDB project (https://github.com/oeg-upm/morph-rdb) which is an implementation of R2RML. Morph-xR2RML supports the data materialization approach (generate RDF instances from data in a database) 
and the query rewriting approach for relational databases only, but it does not support query rewriting (SPARQL to DB native query language rewriting) for MongoDB.

### Limitations
- The generation of RDF collection and containers is supported in all cases (from a list of values resulting of the evaluation of a mixed syntax path typically, from the result of a join query implied by a referencing object map), except in the case of a regular R2RML join query applied to a relational database: the result of the join SQL query cannot be translated into an RDF collection or container.
- Only simple NestedTermMaps are implemented i.e. to qualify RDF terms generated within an RDF collection/container.
More complex nested term maps (with recursive parse using another nested term map and using xrr:reference or rr:template properties) are not supported.
- Named target graphs are not supported.
- The query rewriting is implemented only for RDBs, with the restriction that no mixed syntax paths be used. 
Doing query rewriting with mixed syntax paths is a much more complex problem, that may even not be possible at all. 
Indeed it requires to "revert" expressions such as JSONPath or XPath to retrieve source data base values.

## Build

The application is built using Maven (http://maven.apache.org/). In a shell, CD to the root directory morph-xr2rml, then run the command: ```mvn clean package```.
A jar with all dependencies is generated in morph-xr2rml-dist/target.

Alternatively, you can download a snapshot of the last current version published <a href="https://www.dropbox.com/sh/1xcnvpc7pv6um2i/AAAGpp6oKyZ8pKMxsb6Fgmgja/snapshot/fr/unice/i3s/morph-xr2rml-dist?dl=0">here</a>. Get the jar from the last snapshot directory, for instance 1-0.SNAPSHOT/morph-xr2rml-dist-1.0-20150123.133316-1.jar.


## How to use it?
We provide example databases and corresponding mappings for the MySQL and MongoDB database cases.

#### With MySQL

See directory `morph-xr2rml-dist/example_mysql`
- `testdb_dump.sql` is a dump of the MySQL test database. You may import it into a MySQL instance by running command `mysql -u root -p test < testdb_dump.sql`.
- `morph.properties` provides database connection details.
- `mapping.ttl` contains the xR2RML mapping graph.
- `result.ttl` contains the expected result of applying this mapping to that database.

To run the test, edit morph.properties and change the database url, name, user and password with appropriate values.

From an IDE: locate main class `fr.unice.i3s.morph.xr2rml.engine.MorphRunner` defined in project morph-xr2rml-dist, and run it as a Scala application.

From a command line interface, CD to directory morph-xr2rml-dist and run the application as follows:
```
java -cp target/morph-xr2rml-dist-1.0-SNAPSHOT-jar-with-dependencies.jar \
    fr.unice.i3s.morph.xr2rml.engine.MorphRunner \
    --configDir example_mysql \
    --configFile morph.properties
```

#### With MongoDB

See directory `morph-xr2rml-dist/example_mongo`:
- `testdb_dump.json` is a dump of the MongoDB test database: copy and paste the content of that file into a MongoDB shell window to create the database.
- `morph.properties` provides database connection details.
- `mapping.ttl` contains the xR2RML mapping graph.
- `result.ttl` contains the expected result of applying this mapping to that database.

To run the test, edit morph.properties and change the database url, name, user and password with appropriate values.

From an IDE: locate main class `fr.unice.i3s.morph.xr2rml.engine.MorphRunner` defined in project morph-xr2rml-dist, and run it as a Scala application.

From a command line interface, CD to directory morph-xr2rml-dist and run the application as follows:
```
java -cp target/morph-xr2rml-dist-1.0-SNAPSHOT-jar-with-dependencies.jar \
   fr.unice.i3s.morph.xr2rml.engine.MorphRunner \
   --configDir example_mongo \
   --configFile morph.properties
```
