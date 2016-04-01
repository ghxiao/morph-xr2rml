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
        val factory: IMorphFactory,
        var sparqlQuery: Option[Query]) {

    val logger = Logger.getLogger(this.getClass());

    /**
     * Main function to run the translation of data. Runner must be initialized with a config file.
     */
    def run(): String = {
        val start = System.currentTimeMillis();

        var status: String = null;

        if (this.sparqlQuery.isEmpty) {
            // ----- RDF Triples materialization
            val start = System.currentTimeMillis();

            // Run the queries and generate triples
            factory.getDataTranslator.translateData_Materialization(factory.getMappingDocument)

            // Write the result to the output file
            factory.getMaterializer.materialize
            logger.info("Data materialization duration: " + (System.currentTimeMillis() - start) + "ms.");

        } else {
            // ----- Translate SPARQL query into a target database query
            val rewrittenQuery = factory.getQueryTranslator.translate(sparqlQuery.get);
            if (rewrittenQuery.isDefined) {
                logger.info("SPARQL Query = \n" + sparqlQuery);
                logger.info("------------------ Abstract Query ------------------ = \n" + rewrittenQuery.get.toString);
                logger.info("------------------ Concrete Query ------------------ = \n" + rewrittenQuery.get.toStringConcrete);

                val mapSparqlRewritten = Map((sparqlQuery.get -> rewrittenQuery.get))
                factory.getQueryResultProcessor.translateResult(mapSparqlRewritten);
            } else
                logger.warn("Could not translate the SPARQL into a target query.")
        }

        val end = System.currentTimeMillis();
        logger.info("Total Running Time = " + (end - start) + "ms.");
        logger.info("**********************DONE****************************");
        return status;
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