package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import java.io.FileOutputStream
import java.io.PrintWriter

import org.apache.log4j.Logger

import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QueryExecution
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.query.ResultSetFormatter
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.sparql.core.describe.DescribeBNodeClosure
import com.hp.hpl.jena.sparql.resultset.ResultSetMem

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.engine.IMorphFactory
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.AbstractQuery
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryProcessor

/**
 * Execute the database query and produce the XML SPARQL result set
 */
class MorphMongoQueryProcessor(factory: IMorphFactory) extends MorphBaseQueryProcessor(factory) {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Execute the database query, translate the database results into triples,
     * evaluate the SPARQL query on the resulting graph and save the output to a file.
     *
     * @param sparqlQuery SPARQL query 
     * @param abstractQuery associated AbstractQuery resulting from the translation of sparqlQuery
     * in which the executable target queries have been computed
     * @param syntax the output syntax:  XML or JSON for a SPARQL SELECT or ASK query, or of the 
     * RDF syntaxes for a SPARQL DESCRIBE or CONSTRUCT query
     */
    override def process(sparqlQuery: Query, abstractQuery: AbstractQuery, syntax: String) {

        var start = System.currentTimeMillis()
        factory.getDataTranslator.generateRDFTriples(abstractQuery)
        logger.info("Time for query execution and triples generation = " + (System.currentTimeMillis - start) + "ms.");

        // Late SPARQL evaluation: evaluate the SPARQL query on the result graph
        start = System.currentTimeMillis();
        val qexec: QueryExecution = QueryExecutionFactory.create(sparqlQuery, factory.getMaterializer.model)

        if (sparqlQuery.isAskType) {
            // --- SPARQL ASK
            throw new MorphException("SPARQL ASK not supported")

        } else if (sparqlQuery.isConstructType) {
            // --- SPARQL CONSTRUCT
            val result: Model = qexec.execConstruct
            factory.getMaterializer.serialize(result, syntax)
            qexec.close

        } else if (sparqlQuery.isDescribeType) {
            // --- SPARQL DESCRIBE
            val dh: DescribeBNodeClosure = null
            val result: Model = qexec.execDescribe
            factory.getMaterializer.serialize(result, syntax)
            qexec.close

        } else if (sparqlQuery.isSelectType) {
            // --- SPARQL SELECT

            var resultSet: ResultSet = qexec.execSelect
            if (factory.getProperties.outputDisplay)
                // Create an in-memory result set to display it in tabular format as well as save it to a file
                resultSet = new ResultSetMem(resultSet)

            if (resultSet.hasNext) {
                if (syntax == Constants.OUTPUT_FORMAT_RESULT_XML) {
                    val writer = new PrintWriter(factory.getProperties.outputFilePath, "UTF-8")
                    writer.write(ResultSetFormatter.asXMLString(resultSet))
                    writer.close
                } else if (syntax == Constants.OUTPUT_FORMAT_RESULT_JSON) {
                    val outputStream = new FileOutputStream(factory.getProperties.outputFilePath)
                    ResultSetFormatter.outputAsJSON(outputStream, resultSet)
                    outputStream.close
                } else throw new MorphException("Invalid output result syntax: " + factory.getProperties.outputSyntaxResult)
            }

            if (factory.getProperties.outputDisplay) {
                val rewindable = resultSet.asInstanceOf[ResultSetMem]
                rewindable.rewind
                if (rewindable.hasNext) {
                    if (logger.isInfoEnabled) {
                        logger.info("Result set contains " + rewindable.size + " triples.")
                        logger.info("Tabular result set:\n" + ResultSetFormatter.asText(resultSet))
                    }
                }
            }

            qexec.close
        }

        logger.info("Time for lLate SPARQL query evaluation = " + (System.currentTimeMillis - start) + "ms.");
    }
}