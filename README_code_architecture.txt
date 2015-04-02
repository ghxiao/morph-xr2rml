Below we describe the architecture of classes regarding the treatment of RDBs. This can easily be adapted to the MongoDB case.

base.MorphProperties
    Is a java.util.Properties.
    Holds members for each property definable in the configuration file 
    (defaults to example_mysql/morph.properties, set in project morph-xr2rml-dist, MorphRunner main)

The base.engine.MorphBaseRunnerFactory class is an abstract class that builds an morph.base.engine.MorphBaseRunner.
Database-specific methods are implemented in concrete factory classes like r2rml.rdb.engine.MorphRDBRunnerFactory. 

r2rml.rdb.engine.MorphRDBRunnerFactory < base.engine.MorphBaseRunnerFactory
    Provides methods to initialize:
    - a mapping document (R2RMLMappingDocument): read the xR2RML mapping file into a JENA model, then create an R2RMLMappingDocument
      that consists of a set of classMappings, namely triples maps (R2RMLTriplesMap).
    - a DB connection
    - a data source reader (MorphBaseDataSourceReader) of which concrete class name (MorphRDBDataSourceReader) 
      is passed as contruct parameter. The data source reader provides methods to open, configure, and close the connection, 
      run queries against the DB, and possibly manage strategies to store results to avoid executing the same query several times in the case of joint queries.
    - an unfolder (MorphRDBUnfolder < MorphBaseUnfolder) to create SQL queries using a table name or query
    - a data translator (MorphRDBDataTranslator < MorphBaseDataTranslator) that actually makes the translation: it runs
      SQL queries created by the unfolder, then generates RDF triples from the results.
    - a data materializer (MorphBaseMaterializer) basically consists of a properly initialized (name space, etc.) 
      JENA model, either in mem or db. The model is used to store statements created from subjects, predicates and objects read
      from the database.
    - a query translator, query result writer and query result processor
    
fr.unice.i3s.morph.xr2rml.engine.MorphRunner provide the main class to run the process:
    - Load the properties
    - Create a singleton MorphBaseRunner using the MorphRDBRunnerFactory that creates
      a R2RMLMappingDocument, MorphRDBUnfolder, MorphRDBDataMaterializer and MorphRDBDataTranslator (see above).
    - Run the process using either materialization (MorphBaseRunner.materializeMappingDocuments) or query rewriting
    - Load the RDF graph produced in a JENA model and save a serialization into a file using the requested RDF syntax.
    
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
