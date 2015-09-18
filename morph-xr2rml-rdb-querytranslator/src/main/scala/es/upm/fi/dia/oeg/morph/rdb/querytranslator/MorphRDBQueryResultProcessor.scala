package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import java.io.Writer

import org.apache.log4j.Logger

import com.hp.hpl.jena.query.Query

import es.upm.fi.dia.oeg.morph.base.GenericQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphXMLQueryResultProcessor
import es.upm.fi.dia.oeg.morph.base.sql.ISqlQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

class MorphRDBQueryResultProcessor(
    mappingDocument: R2RMLMappingDocument,
    properties: MorphProperties,
    xmlOutputStream: Writer,
    dataSourceReader: MorphBaseDataSourceReader)

        extends MorphXMLQueryResultProcessor(
            mappingDocument: R2RMLMappingDocument,
            properties: MorphProperties,
            xmlOutputStream: Writer,
            dataSourceReader: MorphBaseDataSourceReader) {

    override val logger = Logger.getLogger(this.getClass().getName());

    def translateResult(mapSparqlSql: Map[Query, GenericQuery]) {
        val start = System.currentTimeMillis();
        var i = 0;
        mapSparqlSql.foreach(mapElement => {
            val sparqlQuery = mapElement._1
            val iQuery = mapElement._2.concreteQuery.asInstanceOf[ISqlQuery]

            // Execution of the concrete SQL query against the database
            val abstractResultSet = this.dataSourceReader.execute(iQuery.toString());
            val columnNames = iQuery.getSelectItemAliases();
            abstractResultSet.setColumnNames(columnNames);

            // Write the XMl result set to the output
            if (i == 0) {
                // The first time, initialize the XML document of the SPARQL result set
                this.preProcess(sparqlQuery);
            }
            this.process(sparqlQuery, abstractResultSet);
            i = i + 1;
        })

        if (i > 0) {
            this.postProcess();
        }

        val end = System.currentTimeMillis();
        logger.info("Result generation time = " + (end - start) + " ms.");
    }

}