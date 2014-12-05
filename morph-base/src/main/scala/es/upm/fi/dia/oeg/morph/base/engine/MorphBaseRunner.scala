package es.upm.fi.dia.oeg.morph.base.engine

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import com.hp.hpl.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.materializer.MaterializerFactory
import com.hp.hpl.jena.query.QueryFactory
import es.upm.fi.dia.oeg.newrqr.RewriterWrapper
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import java.io.OutputStream
import java.io.Writer
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.vocabulary.RDF
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.core.BasicPattern
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.sparql.algebra.op.OpProject
import com.hp.hpl.jena.sparql.algebra.OpAsQuery
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.vocabulary.RDFS
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.exception.MorphException

abstract class MorphBaseRunner(
        mappingDocument: MorphBaseMappingDocument,
        unfolder: MorphBaseUnfolder,
        dataTranslator: Option[MorphBaseDataTranslator],
        val queryTranslator: Option[IQueryTranslator],
        val queryResultTranslator: Option[AbstractQueryResultTranslator],
        var outputStream: Writer) {

    val logger = Logger.getLogger(this.getClass());
    var ontologyFilePath: Option[String] = None;
    var sparqlQuery: Option[Query] = None;
    var mapSparqlSql: Map[Query, IQuery] = Map.empty;

    def setOutputStream(outputStream: Writer) = {
        this.outputStream = outputStream

        if (this.dataTranslator.isDefined) {
            this.dataTranslator.get.materializer.outputStream = outputStream;
        }

        if (this.queryResultTranslator.isDefined) {
            this.queryResultTranslator.get.queryResultWriter.outputStream = outputStream;
        }
    }

    /**
     * Main function to run the translation of data. Runner must be initialized with a config file.
     */
    def run(): String = {
        val start = System.currentTimeMillis();

        var status: String = null;

        if (!this.sparqlQuery.isDefined) {

            // No SPARQL query => materialization mode
            this.materializeMappingDocuments(mappingDocument);

        } else {
            logger.info("sparql query = " + this.sparqlQuery.get);

            //LOADING ONTOLOGY FILE. REWRITE THE SPARQL QUERY IF NECESSARY
            val queries = if (!this.ontologyFilePath.isDefined) {
                List(sparqlQuery.get);
            } else {
                //REWRITE THE QUERY BASED ON THE MAPPINGS AND ONTOLOGY
                logger.info("Rewriting query...");
                val mappedOntologyElements = this.mappingDocument.getMappedClasses();
                val mappedOntologyElements2 = this.mappingDocument.getMappedProperties();
                mappedOntologyElements.addAll(mappedOntologyElements2);

                val queriesAux = RewriterWrapper.rewrite(sparqlQuery.get, ontologyFilePath.get, RewriterWrapper.fullMode, mappedOntologyElements, RewriterWrapper.globalMatchMode);

                logger.info("No of rewriting query result = " + queriesAux.size());
                logger.info("queries = " + queriesAux);
                queriesAux.toList
            }

            //TRANSLATE SPARQL QUERIES INTO SQL QUERIES
            this.mapSparqlSql = this.translateSPARQLQueriesIntoSQLQueries(queries);

            //translate result
            this.queryResultTranslator.get.translateResult(mapSparqlSql);
        }

        val end = System.currentTimeMillis();
        logger.info("Running time = " + (end - start) + "ms.");
        logger.info("**********************DONE****************************");
        return status;
    }

    /**
     * Entry point for the data materialization process
     */
    def materializeMappingDocuments(md: MorphBaseMappingDocument) {

        if (!this.dataTranslator.isDefined) {
            val errorMessage = "Data Translator has not been defined yet.";
            logger.error(errorMessage);
            throw new MorphException(errorMessage)
        }

        val startGeneratingModel = System.currentTimeMillis();
        val cms = md.classMappings;
        cms.foreach(cm => {
            logger.info("===============================================================================");
            logger.info("Starting data materialization of triples map " + cm.id);

            // Run the query and generate triples
            this.dataTranslator.get.generateRDFTriples(cm);
        })

        // Write the result to the output file
        this.dataTranslator.get.materializer.materialize();

        val durationGeneratingModel = (System.currentTimeMillis() - startGeneratingModel);
        logger.info("Data materialization process lasted " + (durationGeneratingModel) + "ms.");
    }

    def readSPARQLFile(sparqQueryFileURL: String) {
        if (this.queryTranslator.isDefined) {
            this.sparqlQuery = Some(QueryFactory.read(sparqQueryFileURL));
        }
    }

    def readSPARQLString(sparqString: String) {
        if (this.queryTranslator.isDefined) {
            this.sparqlQuery = Some(QueryFactory.create(sparqString));
        }
    }

    def translateSPARQLQueriesIntoSQLQueries(sparqlQueries: Iterable[Query]): Map[Query, IQuery] = {
        val sqlQueries = sparqlQueries.map(sparqlQuery => {
            logger.info("SPARQL Query = \n" + sparqlQuery);
            val sqlQuery = this.queryTranslator.get.translate(sparqlQuery);
            logger.info("SQL Query = \n" + sqlQuery);
            (sparqlQuery -> sqlQuery);
        })

        sqlQueries.toMap
    }

    def getQueryTranslator() = {
        queryTranslator.getOrElse(null);
    }

    def getQueryResultWriter() = {
        if (queryResultTranslator.isDefined) {
            queryResultTranslator.get.queryResultWriter
        } else { null }
    }

    def getTranslationResults: java.util.Collection[IQuery] = {
        this.mapSparqlSql.values
    }

}

