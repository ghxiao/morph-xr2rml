Morph-xR2RML is an implementation of the xR2RML mapping language, to describe mappings from relational or non relational databases to RDF. The current implementatiion supports the xR2RML language for relational databases (MySQL, PostgreSQL, MonetDB), and the MongoDB NoSQL document store.

The xR2RML mapping language (https://hal.archives-ouvertes.fr/hal-01066663) is an extension of R2RML (http://www.w3.org/TR/r2rml/) and RML (http://semweb.mmlab.be/rml/spec.html).

Morph-xR2RML was developed by the I3S laboratory (http://www.i3s.unice.fr/) as an extension of the Morph-RDB project (https://github.com/oeg-upm/morph-rdb) which is an implementation of R2RML. Morph-xR2RML supports the data materialization approach (generate RDF instances from data in a database) but, for now, it does not support query translation (SPARQL to DB native query language).

Limitations:
- The generation of RDF collection and containers is supported in all cases (from a list of values resulting of the evaluation of a mixed syntax path typically, from the result of a join query implied by a referencing object map), except in the case of a regular R2RML join query applied to a relational database: the result of the join SQL query cannot be translated into an RDF collection or container.
- Only simple NestedTermMaps are implemented i.e. to qualify RDF terms generated within an RDF collection/container.
More complex nested term maps (with recursive parse using another nested term map and using xrr:reference or rr:template properties) are not supported.
- Named target graphs are not supported.

Examples:
Example mappings are provided in the RDB and MongoDB case along with an example database connection configuration, see:
- mapping.ttl and morph.properties in morph-xr2rml-rdb/examples
- mapping.ttl and morph.properties in morph-xr2rml-jsondoc/examples
