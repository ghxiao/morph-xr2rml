Maven Projects Inheritance:

                morph-core
                    |
             morph-xr2rml-lang
                    |
                morph-base
                  /    \
  morph-xr2rml-mongo   morph-xr2rml-rdb
               |         |
               \         /
            morph-xr2rml-dist


**morph-core**: brings major global definitions: constants, utility classes (properties, exceptions,
    RDF and XML manipulation and serialization), mixed syntax path utilities, SQL machinery.
**morph-xr2rml-lang**: the model representing the xR2RML elements (extended from R2RML).
**morph-base**: abstract classes for major functions of the translation engine: runner factory, runner,
    source reader, data translator, query unfolder etc., + various utility classes. 
**morph-xr2rml-mongo**: implementation of the materialization and query rewriting engine for MongoDB.
**morph-xr2rml-rdb**: implementation of the materialization and query rewriting engine for SQL databases.
**morph-xr2rml-dist**: includes all MongoDb and RDB engines into a single jar, along with a main class (MorphRunner)
    and example databases, mapping and engine configuration files.

-----------------------------------------------------------------------------------------------------

Below we describe the architecture of classes regarding the treatment of RDBs. This can easily be adapted to the MongoDB case.

base.MorphProperties
    Is a java.util.Properties.
    Holds members for each property definable in the configuration file.
    (defaults to example_mysql/morph.properties, set in project morph-xr2rml-dist, MorphRunner main)

The base.engine.MorphBaseRunnerFactory class is an abstract class that builds an morph.base.engine.MorphBaseRunner.
Database-specific methods are implemented in concrete factory classes like r2rml.rdb.engine.MorphRDBRunnerFactory. 

r2rml.rdb.engine.MorphRDBRunnerFactory < base.engine.MorphBaseRunnerFactory
    Provides methods to initialize:
    - a mapping document (R2RMLMappingDocument): read the xR2RML mapping file into a JENA model, then create an R2RMLMappingDocument
      that consists of a set of classMappings, namely triples maps (R2RMLTriplesMap).
    - a DB connection
    - an unfolder (MorphRDBUnfolder < MorphBaseUnfolder) to create SQL queries from a triples map
    - a data source reader (MorphBaseDataSourceReader) provides methods to open, configure and close the connection,
      run queries against the DB, and possibly manage cache strategies.
    - a data translator (MorphRDBDataTranslator < MorphBaseDataTranslator) that actually makes the translation
      in the data materialization case: it runs SQL queries created by the unfolder, then generates RDF triples from the results.
    - a data materializer (MorphBaseMaterializer) basically consists of a properly initialized JENA model (name space, etc.),
      either in mem or db. The model is used to store statements created from subjects, predicates and objects read
      from the database.
    - a query translator and query result processor are used in the query rewriting mode only. The query translator rewrites
      a SPARQL query into an SQL query, the query result processor runs the SQL query against the database and translates the
      SQL result set into an XML SPARQL result set.
    
fr.unice.i3s.morph.xr2rml.engine.MorphRunner provides the main class to run the process:
    - Load the properties
    - Create a singleton MorphBaseRunner using the MorphRDBRunnerFactory that creates
      a R2RMLMappingDocument, MorphRDBUnfolder, MorphRDBDataMaterializer and MorphRDBDataTranslator (see above).
    - Run the process using either materialization (MorphBaseRunner.materializeMappingDocuments) or query rewriting
    - In the data materialization case, load the RDF graph produced in a JENA model and save a serialization into a 
      file using the requested RDF syntax.

Materialization process (MorphBaseRunner.materializeMappingDocuments):
    For each class mapping (R2RMLTriplesMap) of the mapping document (R2RMLMappingDocument),
    - Unfold the class mapping (MorphRDBUnfolder.unfoldTriplesMap): unfolding means to progressively
      build an SQL query by accumulating pieces from different components of the triples map:
      - create the FROM clause with the logical table
      - for each column in the subject, predicate and object maps, add items to the SELECT clause
      - for each column in the parent triples map of each referencing object map, add items of the SELECT clause
      - for each join condition, add an SQL WHERE condition and an alias in the FROM clause for the parent table
      - xR2RML: for each column of each join condition, add items to the SELECT clause
    - Then the data translator (MorphRDBDataTranslator < MorphBaseDataTranslator) runs the query against 
      the database and builds triples from the results. For each row of the result set:
      (1) Create a subject resource, and optionally a graph resource if the subject map contains a rr:graph/rr:graphMap property.
      (2) Loop on each predicate-object map: create a list of resources for the predicates, a list of resources for the objects,
          a list of resources from the subject map of a parent object map in case there are referencing object maps,
          and a list of resources representing target graphs mentioned in the predicate-object map.
      (3) Finally combine all subject, graph, predicate and object resources to generate triples.
      Once all triples have been created in the model, the materializer is used to write them to the output file. 
      The materializer was initially implemented to write triples in the file along their generation, thus avoiding memory space issue.
      However in the case of xR2RML, the RDF lists and containers makes it necessary to wait until the end (when all is generated) 
      to be able to serialize the data to a file.

