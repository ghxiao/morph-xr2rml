Morph-xR2RML is an implementation of the xR2RML mapping language, to describe mappings from relational or non relational databases to RDF. The current implementatiion supports the xR2RML language for relational databases (MySQL, PostgreSQL, MonetDB), and the MongoDB NoSQL document store.

The xR2RML mapping language (https://hal.archives-ouvertes.fr/hal-01066663 V3) is an extension of R2RML (http://www.w3.org/TR/r2rml/) and RML (http://semweb.mmlab.be/rml/spec.html).

Morph-xR2RML was developed by the I3S laboratory (http://www.i3s.unice.fr/) as an extension of the Morph-RDB project (https://github.com/oeg-upm/morph-rdb) which is an implementation of R2RML. Morph-xR2RML supports the data materialization approach (generate RDF instances from data in a database) but, for now, it does not support query translation (SPARQL to DB native query language).

Limitations:
- The generation of RDF collection and containers is supported in all cases (from a list of values resulting of the evaluation of a mixed syntax path typically, from the result of a join query implied by a referencing object map), except in the case of a regular R2RML join query applied to a relational database: the result of the join SQL query cannot be translated into an RDF collection or container.
- Only simple NestedTermMaps are implemented i.e. to qualify RDF terms generated within an RDF collection/container.
More complex nested term maps (with recursive parse using another nested term map and using xrr:reference or rr:template properties) are not supported.
- Named target graphs are not supported.

Examples:
---------
Example database and mapping are provided for the MySQL and MongoDB database cases, see:

MySQL: see morph-xr2rml-rdb/examples
- testdb_dump.sql is a dump of the MySQL test database
- morph.properties provide database connection details
- mapping.ttl contains the xR2RML mapping graph
To run the test, edit morph.properties and change database url, name, user and password with appropriate values.
Then run main class es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunner (defined in project morph-xr2rml-rdb).

MongoDB: see morph-xr2rml-jsondoc/examples
- testdb_dump.json is a dump of the MongoDB test database: run the commands in the MongoDB shell window to create the documents
- morph.properties provide database connection details
- mapping.ttl contains the xR2RML mapping graph
To run the test, edit morph.properties and change database url, name, user and password with appropriate values.
Then run main class fr.unice.i3s.morph.xr2rml.jsondoc.engine.MorphJsondocRunner (defined in project morph-xr2rml-jsondoc).

A simple way to test those examples is to import the project into Eclipse and run the main classes mentioned above as Scala application.
