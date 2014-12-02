Morph-xR2RML is an implementation of the xR2RML mapping language, to describe mappings from relational or non relational databases to RDF. The current implementatiion supports the xR2RML language for relational databases (MySQL, PostgreSQL, MonetDB), and the MongoDB NoSQL document store.

The xR2RML mapping language (https://hal.archives-ouvertes.fr/hal-01066663) is an extension of R2RML (http://www.w3.org/TR/r2rml/) and RML (http://semweb.mmlab.be/rml/spec.html).

Morph-xR2RML was developed by the I3S laboratory (http://www.i3s.unice.fr/) as an extension of the Morph-RDB project (https://github.com/oeg-upm/morph-rdb) which is an implementation of R2RML. Morph-xR2RML supports the data materialization approach (generate RDF instances from data in a database) but, for now, it does not support query translation (SPARQL to DB native query language).

Limitations:
- The generation of RDF collection and containers is not supported in the case of a regular join query in an RDB.
- As in Morph-RDB, named target graphs are not supported.
- NestedTermMaps are not implemented. As a result, collections or containers evaluated from a value without join will always contain literal with no term type, language tag nor datatype. Note this has no impact on collections or containers resulting from a join condition, as they always contain IRIs.

Examples:
Example mappings are provided in the RDB and MongoDB case along with an example database connection configuration, see:
- mapping.ttl and morph.properties in morph-xr2rml-rdb/examples
- mapping.ttl and morph.properties in morph-xr2rml-jsondoc/examples
