package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import java.io.Writer
import scala.collection.JavaConversions.asScalaBuffer
import org.apache.log4j.Logger
import com.hp.hpl.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.AbstractQuery
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.TermMapResult
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.querytranslator.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseProjectionGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphXMLQueryResultProcessor
import es.upm.fi.dia.oeg.morph.base.sql.ISqlQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoResultSet
import fr.unice.i3s.morph.xr2rml.mongo.engine.MorphMongoDataTranslator

class MorphMongoQueryResultProcessor(
    mappingDocument: R2RMLMappingDocument,
    properties: MorphProperties,
    dataSourceReader: MorphBaseDataSourceReader,
    dataTranslator: MorphMongoDataTranslator,
    xmlOutputStream: Writer)

        extends MorphXMLQueryResultProcessor(
            mappingDocument: R2RMLMappingDocument,
            properties: MorphProperties,
            xmlOutputStream: Writer) {

    override val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Execute the query and translate the results from the database into triples.<br>
     */
    def translateResult(mapSparqlSql: Map[Query, AbstractQuery]) {
        val start = System.currentTimeMillis();

        var i = 0;
        mapSparqlSql.foreach(mapElement => {
            val sparqlQuery = mapElement._1
            val genQuery = mapElement._2.head

            dataTranslator.generateRDFTriples(genQuery)
        })

        if (i > 0) {
            this.postProcess();
        }

        val end = System.currentTimeMillis();
        logger.info("Result generation time = " + (end - start) + "ms.");
    }

    override def process(sparqlQuery: Query, resultSet: MorphBaseResultSet) = {
        logger.info("Executing process")
    }
}