package es.upm.fi.dia.oeg.morph.base.engine

import java.io.FileOutputStream

import org.apache.log4j.Logger

import com.hp.hpl.jena.sparql.core.describe.DescribeHandlerRegistry

import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.materializer.ExtendedDescribeBNodeCloserFactory
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryResultProcessor
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

abstract class MorphBaseRunnerFactory extends IMorphFactory {

    var properties: MorphProperties = null

    var connection: GenericConnection = null

    var mappingDocument: R2RMLMappingDocument = null

    var unfolder: MorphBaseUnfolder = null

    var dataSourceReader: MorphBaseDataSourceReader = null

    var materializer: MorphBaseMaterializer = null

    var dataTranslator: MorphBaseDataTranslator = null

    var queryTranslator: MorphBaseQueryTranslator = null

    var queryResultProcessor: MorphBaseQueryResultProcessor = null

    override def getProperties: MorphProperties = properties

    override def getConnection: GenericConnection = connection

    override def getMappingDocument: R2RMLMappingDocument = mappingDocument

    override def getUnfolder: MorphBaseUnfolder = unfolder

    override def getDataSourceReader: MorphBaseDataSourceReader = dataSourceReader

    override def getMaterializer: MorphBaseMaterializer = materializer

    override def getDataTranslator: MorphBaseDataTranslator = dataTranslator

    override def getQueryTranslator: MorphBaseQueryTranslator = queryTranslator

    override def getQueryResultProcessor: MorphBaseQueryResultProcessor = queryResultProcessor

    val logger = Logger.getLogger(this.getClass());

    def createRunner: MorphBaseRunner = {
        if (logger.isDebugEnabled) logger.debug("Creating MorphBaseRunner")
        new MorphBaseRunner(this)
    }

    def createConnection: GenericConnection

    def createUnfolder: MorphBaseUnfolder

    def createDataSourceReader: MorphBaseDataSourceReader

    def createDataTranslator: MorphBaseDataTranslator

    def createQueryTranslator: MorphBaseQueryTranslator

    def createQueryResultProcessor: MorphBaseQueryResultProcessor;

    private def createMaterializer: MorphBaseMaterializer = {

        // Initialize the SPARQL DESCRIBE handler
        DescribeHandlerRegistry.get.add(new ExtendedDescribeBNodeCloserFactory)

        val jenaMode = this.getProperties.jenaMode;
        val materializer = MorphBaseMaterializer(this, jenaMode)
        val prefix = this.getMappingDocument.mappingDocumentPrefixMap
        if (prefix != null)
            materializer.setModelPrefixMap(prefix);
        materializer
    }
}

object MorphBaseRunnerFactory {
    def createFactory(properties: MorphProperties): MorphBaseRunnerFactory = {

        val factory = Class.forName(properties.runnerFactoryClassName).newInstance().asInstanceOf[MorphBaseRunnerFactory]

        // DO NOT CHANGE ORDER - Objects must be created in this order because of their dependencies
        factory.properties = properties
        factory.connection = factory.createConnection
        factory.mappingDocument = R2RMLMappingDocument(factory.properties, factory.connection);
        factory.unfolder = factory.createUnfolder
        factory.dataSourceReader = factory.createDataSourceReader
        factory.materializer = factory.createMaterializer
        factory.dataTranslator = factory.createDataTranslator
        factory.queryTranslator = factory.createQueryTranslator
        factory.queryResultProcessor = factory.createQueryResultProcessor
        factory
    }
}
