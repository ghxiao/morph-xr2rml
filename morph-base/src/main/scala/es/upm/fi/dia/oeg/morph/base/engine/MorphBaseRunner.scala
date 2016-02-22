package es.upm.fi.dia.oeg.morph.base.engine

import java.io.BufferedWriter
import java.io.FileWriter
import java.io.IOException
import java.io.PrintWriter
import java.io.Writer

import org.apache.log4j.Logger

import com.hp.hpl.jena.query.Query

import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryResultProcessor
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

class MorphBaseRunner(
        val mappingDocument: R2RMLMappingDocument,
        val unfolder: MorphBaseUnfolder,
        val dataTranslator: MorphBaseDataTranslator,
        val queryTranslator: MorphBaseQueryTranslator,
        val queryResultProcessor: MorphBaseQueryResultProcessor,
        var outputStream: Writer,
        var sparqlQuery: Option[Query]) {

    val logger = Logger.getLogger(this.getClass());

    /**
     * Main function to run the translation of data. Runner must be initialized with a config file.
     */
    def run(): String = {
        val start = System.currentTimeMillis();

        var status: String = null;

        if (this.sparqlQuery.isEmpty) {
            // RDF Triples materialization
            this.materializeMappingDocuments(mappingDocument);

        } else {
            // Query translation mode
            if (this.queryTranslator == null) {
                val errorMessage = "No query translator initialized. Cannot run in query rewriting mode.";
                logger.fatal(errorMessage);
                throw new MorphException(errorMessage)
            }
            if (this.queryResultProcessor == null) {
                val errorMessage = "No query result processor initialized. Cannot run in query rewriting mode.";
                logger.fatal(errorMessage);
                throw new MorphException(errorMessage)
            }

            // Translate SPARQL query into SQL
            val rewrittenQuery = this.queryTranslator.translate(sparqlQuery.get);
            logger.info("SPARQL Query = \n" + sparqlQuery);
            logger.info("Rewrtitten Abstract Query = \n" + rewrittenQuery.toString);
            logger.info("Rewrtitten Concrete Query = \n" + rewrittenQuery.toStringConcrete);

            val mapSparqlRewritten = Map((sparqlQuery.get -> rewrittenQuery))
            this.queryResultProcessor.translateResult(mapSparqlRewritten);
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

        if (this.dataTranslator == null) {
            val errorMessage = "No data translator defined. Cannot run in data materialization.";
            logger.fatal(errorMessage);
            throw new MorphException(errorMessage)
        }
        val start = System.currentTimeMillis();

        // Run the query and generate triples
        this.dataTranslator.translateData_Materialization(mappingDocument)

        // Write the result to the output file
        this.dataTranslator.materializer.materialize();

        val duration = (System.currentTimeMillis() - start);
        logger.info("Data materialization process lasted " + (duration) + "ms.");
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