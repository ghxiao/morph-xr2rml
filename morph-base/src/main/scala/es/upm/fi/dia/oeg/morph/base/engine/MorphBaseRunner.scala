package es.upm.fi.dia.oeg.morph.base.engine

import java.io.BufferedWriter
import java.io.FileWriter
import java.io.IOException
import java.io.PrintWriter
import java.io.Writer

import scala.collection.JavaConversions.asJavaCollection
import scala.collection.JavaConversions.asScalaBuffer

import org.apache.log4j.Logger

import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QueryFactory

import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

class MorphBaseRunner(
        mappingDocument: R2RMLMappingDocument,
        unfolder: MorphBaseUnfolder,
        dataTranslator: Option[MorphBaseDataTranslator],
        val queryTranslator: Option[IQueryTranslator],
        val queryResultTranslator: Option[AbstractQueryResultTranslator],
        var outputStream: Writer,
        var sparqlQuery: Option[Query]) {

    val logger = Logger.getLogger(this.getClass());

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
            //Translate SPARQL query into SQL
            val mapSparqlSql = this.translateSPARQLQueriesIntoSQLQueries(List(sparqlQuery.get));
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
    def materializeMappingDocuments(md: R2RMLMappingDocument) {

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

    private def translateSPARQLQueriesIntoSQLQueries(sparqlQueries: Iterable[Query]): Map[Query, IQuery] = {
        val sqlQueries = sparqlQueries.map(sparqlQuery => {
            logger.info("SPARQL Query = \n" + sparqlQuery);
            val sqlQuery = this.queryTranslator.get.translate(sparqlQuery);
            logger.info("SQL Query = \n" + sqlQuery);
            (sparqlQuery -> sqlQuery);
        })

        sqlQueries.toMap
    }
}

object MorphBaseRunner {
    val logger = Logger.getLogger(this.getClass());

    def erasefile(file: String) {
        try {
            var out = new PrintWriter(new BufferedWriter(new FileWriter(file, false)))
            out.print("")
            out.flush()
            out.close()
        } catch {
            case e: IOException => { logger.info("Error with file " + file); }
        }
    }
}