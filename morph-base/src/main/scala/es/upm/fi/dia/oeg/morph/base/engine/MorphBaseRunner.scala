package es.upm.fi.dia.oeg.morph.base.engine

import java.io.BufferedWriter
import java.io.FileWriter
import java.io.IOException
import java.io.PrintWriter

import org.apache.log4j.Logger

import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.query.Query

class MorphBaseRunner(val factory: IMorphFactory) {

    val logger = Logger.getLogger(this.getClass());

    def run = {
        if (factory.getProperties.queryFilePath.isDefined) {
            val sparqlQuery = QueryFactory.read(factory.getProperties.queryFilePath.get)
            this.runQuery(sparqlQuery)
        } else
            this.runMaterialization
    }

    /**
     * RDF Triples materialization
     */
    def runMaterialization = {
        logger.info("Running graph materialization...")
        val startTime = System.currentTimeMillis()

        // Run the queries and generate triples
        factory.getDataTranslator.translateData_Materialization(factory.getMappingDocument)

        // Write the result to the output file
        factory.getMaterializer.materialize
        conclude(startTime)
    }

    /**
     * Run with a SPARQL query
     */
    def runQuery(sparqlQuery: Query) = {
        logger.info("=================================================================================================================")
        logger.info("Running query translation...")
        val startTime = System.currentTimeMillis()

        val rewrittenQuery = factory.getQueryTranslator.translate(sparqlQuery)
        if (rewrittenQuery.isDefined) {
            logger.info("SPARQL Query = \n" + sparqlQuery);
            logger.info("------------------ Abstract Query ------------------ = \n" + rewrittenQuery.get.toString);
            logger.info("------------------ Concrete Query ------------------ = \n" + rewrittenQuery.get.toStringConcrete);

            factory.getQueryResultProcessor.translateResult(sparqlQuery, rewrittenQuery.get)
        } else
            logger.warn("Could not translate the SPARQL into a target query.")

        conclude(startTime)
    }

    private def conclude(startTime: Long) = {
        val endTime = System.currentTimeMillis();
        logger.warn("Execution time = " + (endTime - startTime) + "ms.");
        logger.warn("**********************DONE****************************");
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