package es.upm.fi.dia.oeg.morph.rdb.querytranslator
import scala.collection.JavaConversions.asScalaBuffer
import java.io.Writer
import java.util.regex.Matcher
import java.util.regex.Pattern
import org.apache.log4j.Logger
import com.hp.hpl.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.TemplateUtility
import es.upm.fi.dia.oeg.morph.base.TermMapResult
import es.upm.fi.dia.oeg.morph.base.UnionOfGenericQueries
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphXMLQueryResultProcessor
import es.upm.fi.dia.oeg.morph.base.sql.ISqlQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseProjectionGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.IQueryTranslator

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
    def translateResult(mapSparqlSql: Map[Query, UnionOfGenericQueries]) {
    }

    override def process(sparqlQuery: Query, resultSet: MorphBaseResultSet) = {
        logger.info("Executing process")
    }
}