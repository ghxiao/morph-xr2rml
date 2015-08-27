package es.upm.fi.dia.oeg.morph.base.engine

import java.io.PrintWriter
import java.io.StringWriter
import java.io.Writer

import org.apache.log4j.Logger

import com.hp.hpl.jena.query.QueryFactory

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.materializer.MaterializerFactory
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

abstract class MorphBaseRunnerFactory {
    val logger = Logger.getLogger(this.getClass());

    def createRunner(configurationDirectory: String, configurationFile: String): MorphBaseRunner = {
        val configurationProperties = MorphProperties.apply(
            configurationDirectory, configurationFile);
        this.createRunner(configurationProperties);
    }

    def createRunner(properties: MorphProperties): MorphBaseRunner = {

        logger.info("Creating MorphBaseRunner")

        // Building CONNECTION
        val connection = this.createConnection(properties);

        // Building MAPPING DOCUMENT
        val mappingDocument = R2RMLMappingDocument(properties.mappingDocumentFilePath, properties, connection);

        // Building UNFOLDER
        val unfolder = this.createUnfolder(properties, mappingDocument);

        // Building MATERIALIZER
        val outputStream: Writer =
            if (properties.outputFilePath.isDefined)
                new PrintWriter(properties.outputFilePath.get, "UTF-8")
            else new StringWriter
        val materializer = this.buildMaterializer(properties, mappingDocument, outputStream);

        // Building DATA SOURCE READER (query rewriting mode only)
        val dataSourceReader = this.createDataSourceReader(properties, connection);

        // Building DATA TRANSLATOR
        val dataTranslator = try {
            Some(this.createDataTranslator(mappingDocument, materializer, unfolder, dataSourceReader, connection, properties))
        } catch {
            case e: Exception => {
                logger.error("Error building data translator: " + e.getMessage());
                throw e
            }
        }

        // ---------------------------------------------------------------------------------
        // The Query Translator, Query Result Writer, Result Processor, are only applicable 
        // in the case of query rewriting access mode, i.e. not in data materialization.
        // ---------------------------------------------------------------------------------

        // Building QUERY TRANSLATOR
        logger.info("Building query translator...");
        val queryTranslator =
            try {
                val qtAux = this.createQueryTranslator(properties, mappingDocument, connection)
                if (qtAux != null)
                    Some(qtAux)
                else None
            } catch {
                case e: Exception => {
                    logger.warn("Error building query translator: " + e.getMessage());
                    e.printStackTrace()
                    None
                }
            }

        // Building QUERY RESULT WRITER and RESULT PROCESSOR
        val resultProcessor = this.createQueryResultTranslator(properties, mappingDocument, connection, dataSourceReader, queryTranslator, outputStream)

        // ---------------------------------------------------------------------------------
        // Creation of final runner object
        // ---------------------------------------------------------------------------------

        val sparqlQuery =
            if (properties.queryFilePath.isDefined)
                Some(QueryFactory.read(properties.queryFilePath.get))
            else None
        val runner = new MorphBaseRunner(mappingDocument, unfolder, dataTranslator, queryTranslator, resultProcessor, outputStream, sparqlQuery)
        runner;
    }

    def createConnection(configurationProperties: MorphProperties): GenericConnection

    def createUnfolder(properties: MorphProperties, md: R2RMLMappingDocument): MorphBaseUnfolder

    def createDataSourceReader(properties: MorphProperties, connection: GenericConnection): MorphBaseDataSourceReader

    def createDataTranslator(md: R2RMLMappingDocument, materializer: MorphBaseMaterializer, unfolder: MorphBaseUnfolder, dataSourceReader: MorphBaseDataSourceReader, connection: GenericConnection, properties: MorphProperties): MorphBaseDataTranslator

    def createQueryTranslator(properties: MorphProperties, md: R2RMLMappingDocument, connection: GenericConnection): IQueryTranslator

    def createQueryResultTranslator(
        properties: MorphProperties,
        md: R2RMLMappingDocument,
        connection: GenericConnection,
        dataSourceReader: MorphBaseDataSourceReader,
        queryTranslator: Option[IQueryTranslator],
        outputStream: Writer): Option[AbstractQueryResultTranslator];

    private def buildMaterializer(configurationProperties: MorphProperties, mappingDocument: R2RMLMappingDocument, outputStream: Writer): MorphBaseMaterializer = {
        val jenaMode = configurationProperties.jenaMode;
        val materializer = MaterializerFactory.create(outputStream, jenaMode);
        val mappingDocumentPrefixMap = mappingDocument.mappingDocumentPrefixMap;
        if (mappingDocumentPrefixMap != null) {
            materializer.setModelPrefixMap(mappingDocumentPrefixMap);
        }
        materializer
    }
}
