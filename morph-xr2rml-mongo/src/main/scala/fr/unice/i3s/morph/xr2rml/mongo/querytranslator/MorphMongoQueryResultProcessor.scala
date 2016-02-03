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

class MorphMongoQueryResultProcessor(
    mappingDocument: R2RMLMappingDocument,
    properties: MorphProperties,
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
    }

    override def process(sparqlQuery: Query, resultSet: MorphBaseResultSet) = {
        logger.info("Executing process")
    }
}