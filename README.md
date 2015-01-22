## What is it?
Morph-xR2RML is an implementation of the xR2RML mapping language, to describe mappings from relational or non relational databases to RDF. The current implementatiion supports the xR2RML language for relational databases (MySQL, PostgreSQL, MonetDB), and the MongoDB NoSQL document store.

The xR2RML mapping language (https://hal.archives-ouvertes.fr/hal-01066663 V3) is an extension of R2RML (http://www.w3.org/TR/r2rml/) and RML (http://semweb.mmlab.be/rml/spec.html).

Morph-xR2RML was developed by the I3S laboratory (http://www.i3s.unice.fr/) as an extension of the Morph-RDB project (https://github.com/oeg-upm/morph-rdb) which is an implementation of R2RML. Morph-xR2RML supports the data materialization approach (generate RDF instances from data in a database) but, for now, it does not support query translation (SPARQL to DB native query language).

### Limitations
- The generation of RDF collection and containers is supported in all cases (from a list of values resulting of the evaluation of a mixed syntax path typically, from the result of a join query implied by a referencing object map), except in the case of a regular R2RML join query applied to a relational database: the result of the join SQL query cannot be translated into an RDF collection or container.
- Only simple NestedTermMaps are implemented i.e. to qualify RDF terms generated within an RDF collection/container.
More complex nested term maps (with recursive parse using another nested term map and using xrr:reference or rr:template properties) are not supported.
- Named target graphs are not supported.

## Build

#### Using the command line interface
The application is built using Maven (http://maven.apache.org/). In a shell, CD to the root directory morph-xr2rml, then run the command: ```mvn clean package```

#### Using an IDE
Another simple way to compile the application is to import the parent project (directory morph-xr2rml) as a Maven project into your favorite IDE.

## Examples
Example databases and corresponding mappings are provided for the MySQL and MongoDB database cases, see:

#### With MySQL

See directory `morph-xr2rml-rdb/examples`
- `testdb_dump.sql` is a dump of the MySQL test database,
- `morph.properties` provides database connection details,
- `mapping.ttl` contains the xR2RML mapping graph.
- `result.ttl` contains the expected result of applying this mapping to that database.

To run the test, edit morph.properties and change the database url, name, user and password with appropriate values.

From an IDE: locate main class `es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunner` defined in project morph-xr2rml-rdb, and run it as a Scala application.

From a command line interface, run the application as follows:
```
cd morph-xr2rml-rdb
java -cp target/morph-xr2rml-rdb-1.0-SNAPSHOT-jar-with-dependencies.jar es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunner
```

You may also use command line options --configDir and --configFile to specify your own configuration directory and configuration file.

#### With MongoDB

See directory `morph-xr2rml-jsondoc/examples`:
- `testdb_dump.json` is a dump of the MongoDB test database: run the commands in the MongoDB shell window to create the documents,
- `morph.properties` provides database connection details,
- `mapping.ttl` contains the xR2RML mapping graph.
- `result.ttl` contains the expected result of applying this mapping to that database.

To run the test, edit morph.properties and change the database url, name, user and password with appropriate values.

From an IDE: locate main class `fr.unice.i3s.morph.xr2rml.jsondoc.engine.MorphJsondocRunner` defined in project morph-xr2rml-jsondoc, and run it as a Scala application.

From a command line interface, run the application as follows:
```
cd morph-xr2rml-jsondoc
java -cp target/morph-xr2rml-jsondoc-1.0-SNAPSHOT-jar-with-dependencies.jar fr.unice.i3s.morph.xr2rml.jsondoc.engine.MorphJsondocRunner
```

You may also use command line options --configDir and --configFile to specify your own configuration directory and configuration file.
