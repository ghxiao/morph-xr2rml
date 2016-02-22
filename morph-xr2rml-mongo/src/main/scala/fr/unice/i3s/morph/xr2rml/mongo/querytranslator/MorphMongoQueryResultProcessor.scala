package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import java.io.Writer
import org.apache.log4j.Logger
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QueryExecution
import com.hp.hpl.jena.query.QueryExecutionFactory
import com.hp.hpl.jena.query.ResultSet
import com.hp.hpl.jena.query.ResultSetFormatter
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.query.GenericQuery
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQuery
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryResultProcessor
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoDataTranslator
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractAtomicQuery
import es.upm.fi.dia.oeg.morph.base.query.MorphAbstractQueryInnerJoinRef

/**
 * Execute the database query and produce the XML SPARQL result set
 */
class MorphMongoQueryResultProcessor(
    mappingDocument: R2RMLMappingDocument,
    properties: MorphProperties,
    dataSourceReader: MorphBaseDataSourceReader,
    dataTranslator: MorphMongoDataTranslator,
    output: Writer)

        extends MorphBaseQueryResultProcessor(
            mappingDocument: R2RMLMappingDocument,
            properties: MorphProperties,
            output: Writer) {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Execute the database query, translate the database results into triples,
     * evaluate the SPARQL query on the resulting graph and save the XML output to a file.
     */
    override def translateResult(mapSparqlSql: Map[Query, MorphAbstractQuery]) {
        val start = System.currentTimeMillis();

        mapSparqlSql.foreach(mapElement => {
            val sparqlQuery: Query = mapElement._1

            if (mapElement._2.isInstanceOf[MorphAbstractAtomicQuery]) {
                
                // @TODO For now just one result GenericQuery
                val genQuery: GenericQuery = mapElement._2.targetQuery(0).asInstanceOf[GenericQuery]

                // Generate triples matching the triple pattern in the current model
                // @TODO: for now this works fine with 
                dataTranslator.translateDate_QueryRewriting(genQuery, None)
                
            } else if (mapElement._2.isInstanceOf[MorphAbstractQueryInnerJoinRef]) {
                
                // @TODO For now just one result GenericQuery
                val q = mapElement._2.asInstanceOf[MorphAbstractQueryInnerJoinRef]
                
                val childQuery: GenericQuery = q.child.targetQuery(0)
                val parentQuery: GenericQuery = q.parent.targetQuery(0)
                dataTranslator.translateDate_QueryRewriting(childQuery, Some(parentQuery))
            }

            // Evaluate the SPARQL query on the result graph
            val qexec: QueryExecution = QueryExecutionFactory.create(sparqlQuery, this.dataTranslator.materializer.model)
            val resultSet: ResultSet = qexec.execSelect();
            while (resultSet.hasNext()) {
                val strResultSet = ResultSetFormatter.asXMLString(resultSet)
                if (logger.isDebugEnabled()) logger.debug("Writing query result document:\n" + strResultSet)
                output.write(strResultSet)
            }

            this.output.flush()
            this.output.close()
        })

        val end = System.currentTimeMillis();
        logger.info("Result generation time = " + (end - start) + "ms.");
    }

    override def preProcess(sparqlQuery: Query): Unit = {}

    override def process(sparqlQuery: Query, resultSet: MorphBaseResultSet): Unit = {}

    override def postProcess(): Unit = {}

    override def getOutput(): Object = { null }
}