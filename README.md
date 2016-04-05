# What is it?
Morph-xR2RML is an implementation of the [xR2RML mapping language](https://hal.archives-ouvertes.fr/hal-01141686) that enables the description of mappings from relational or non relational databases to RDF. xR2RML is an extension of [R2RML](http://www.w3.org/TR/r2rml/) and [RML](http://semweb.mmlab.be/rml/spec.html).

Morph-xR2RML comes with connectors for relational databases (MySQL, PostgreSQL, MonetDB) and the MongoDB NoSQL document store.

Two running modes are available:
- the *graph materialization* mode creates all possible RDF triples at once.
- the *query rewriting* mode translates a SPARQL query into a target database query and returns a SPARQL anwser.
Both modes are implemented for RDBs as well as MongoDB.

Morph-xR2RML was developed by the [I3S laboratory](http://www.i3s.unice.fr/) as an extension of the [Morph-RDB project](https://github.com/oeg-upm/morph-rdb) which is an implementation of R2RML. It is made available under the Apache 2.0 License.

#### SPARQL-to-SQL
The SPARQL-to-SQL rewriting is an adaptation of the former Morph-RDB implementation, it supports SELECT queries.

#### SPARQL-to-MongoDB
The SPARQL-to-MongoDB rewriting is a fully new component, it supports SELECT, CONSTRUCT and DESCRIBE queries.

To the best of our knowledge, Morph-xR2RML is the **first tool to support the querying of arbitrary MongoDB documents using SPARQL**.


### Publications
[1] F. Michel, C. Faron-Zucker, and J. Montagnat. A Generic Mapping-Based Query Translation from SPARQL to Various Target Database Query Languages.
In Proceedings of the 12th International Confenrence on Web Information Systems and Technologies (WEBIST 2016), Roma, Italy, 2016.

[2] F. Michel, C. Faron-Zucker, and J. Montagnat. Mapping-based SPARQL access to a MongoDB database. Technical report, CNRS, 2016. 
https://hal.archives-ouvertes.fr/hal-01245883.

[3] F. Michel, L. Djimenou, C. Faron-Zucker, and J. Montagnat. Translation of Relational and Non-Relational Databases into RDF with xR2RML.
In Proceedings of the 11th International Confenrence on Web Information Systems and Technologies (WEBIST 2015), Lisbon, Portugal, 2015.

[4] F. Michel, L. Djimenou, C. Faron-Zucker, and J. Montagnat. xR2RML: Relational and Non-Relational Databases to RDF Mapping Language.
Technical report, CNRS, 2015. https://hal.archives-ouvertes.fr/hal-01066663

### Limitations

##### xR2RML Language support
- The generation of RDF collection and containers is supported in all cases (from a list of values resulting of the evaluation of a mixed syntax path typically, from the result of a join query implied by a referencing object map), except in the case of a regular R2RML join query applied to a relational database: the result of the join SQL query cannot be translated into an RDF collection or container.
- Only simple NestedTermMaps are implemented i.e. to qualify RDF terms generated within an RDF collection/container.
More complex nested term maps (with recursive parsing using another nested term map and using xrr:reference or rr:template properties) are not supported.
- Named target graphs are not supported.

##### Query rewriting 
The query rewriting is implemented for RDBs and MongoDB, with the restriction that _no mixed syntax paths be used_.
Doing query rewriting with mixed syntax paths is a much more complex problem, that may not be possible in all situations.
(it would require to "revert" expressions such as JSONPath or XPath to retrieve source data base values).

Iterators (rml:iterator) are not fully managed in the MongoDB query rewriting mode. Also, only one join condition is supported in a referencing object map.


# Code description

See a detailed [description of the project code architecture](README_code_architecture.md).


# Download and Build

You can download the last snapshot published in [this repository](https://www.dropbox.com/sh/1xcnvpc7pv6um2i/AAAGpp6oKyZ8pKMxsb6Fgmgja/snapshot/fr/unice/i3s/morph-xr2rml-dist?dl=0).

Alternatively, you can build the application using [Maven](http://maven.apache.org/): in a shell, CD to the root directory morph-xr2rml, then run the command: ```mvn clean package```.
A jar with all dependencies is generated in morph-xr2rml-dist/target.


# How to use it?
We provide example databases and corresponding mappings for the MySQL and MongoDB database cases.

### With MongoDB

In directories `morph-xr2rml-dist/example_mongo` and `morph-xr2rml-dist/example_mongo_rewriting`:
- `testdb_dump.json` is a dump of the MongoDB test database: copy and paste the content of that file into a MongoDB shell window to create the database;
- `morph.properties` provides database connection details;
- `mapping.ttl` contains the xR2RML mapping graph;
- `result.ttl` contains the expected result;
- `query.sparql` contains a SPARQL query to be executed against the test database, and `morph.properties` provides the name of that file (property `query.file.path`).

Edit `morph.properties` and change the database url, name, user and password with appropriate values.

**From an IDE**: In project morph-xr2rml-dist locate main class `fr.unice.i3s.morph.xr2rml.engine.MorphRunner`, and run it as a Scala application with arguments `--configDir example_mongo` or `--configDir example_mongo_rewriting`, and `--configFile morph.properties`.

**From a command line interface**, CD to directory morph-xr2rml-dist and run the application as follows:
```
java -cp target/morph-xr2rml-dist-1.0-SNAPSHOT-jar-with-dependencies.jar \
   fr.unice.i3s.morph.xr2rml.engine.MorphRunner \
   --configDir example_mongo \
   --configFile morph.properties
```

### With MySQL

In directories `morph-xr2rml-dist/example_mysql` and `morph-xr2rml-dist/example_mysql_rewriting`:
- `testdb_dump.sql` is a dump of the MySQL test database. You may import it into a MySQL instance by running command `mysql -u root -p test < testdb_dump.sql`;
- `morph.properties` provides database connection details;
- `mapping.ttl` contains the xR2RML mapping graph;
- `result.ttl` contains the expected result of applying this mapping to that database;
- `query.sparql` contains a SPARQL query to be executed against the test database, and `morph.properties` provides the name of that file (property `query.file.path`).

Edit `morph.properties` and change the database url, name, user and password with appropriate values.

**From an IDE**: In project morph-xr2rml-dist locate main class `fr.unice.i3s.morph.xr2rml.engine.MorphRunner`, and run it as a Scala application with arguments `--configDir example_mysql` or `--configDir example_mysql_rewriting`, and `--configFile morph.properties`.

**From a command line interface**, CD to directory morph-xr2rml-dist and run the application as follows:
```
java -cp target/morph-xr2rml-dist-1.0-SNAPSHOT-jar-with-dependencies.jar \
    fr.unice.i3s.morph.xr2rml.engine.MorphRunner \
    --configDir example_mysql \
    --configFile morph.properties
```
