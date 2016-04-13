package es.upm.fi.dia.oeg.morph.base.engine

import org.apache.log4j.Logger

import com.hp.hpl.jena.sparql.core.describe.DescribeHandlerRegistry

import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.materializer.ExtendedDescribeBNodeCloserFactory
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryProcessor
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

/**
 * @author Freddy Priyatna
 * @author Franck Michel, I3S laboratory
 */
abstract class MorphBaseRunnerFactory extends IMorphFactory {

    var properties: MorphProperties = null

    var mappingDocument: R2RMLMappingDocument = null

    var connection: GenericConnection = null

    var unfolder: MorphBaseUnfolder = null

    var dataSourceReader: MorphBaseDataSourceReader = null

    var materializer: MorphBaseMaterializer = null

    var dataTranslator: MorphBaseDataTranslator = null

    var queryTranslator: MorphBaseQueryTranslator = null

    var queryProcessor: MorphBaseQueryProcessor = null

    override def getProperties: MorphProperties = properties

    override def getConnection: GenericConnection = connection

    override def getMappingDocument: R2RMLMappingDocument = mappingDocument

    override def getUnfolder: MorphBaseUnfolder = unfolder

    override def getDataSourceReader: MorphBaseDataSourceReader = dataSourceReader

    override def getMaterializer: MorphBaseMaterializer = materializer

    override def getDataTranslator: MorphBaseDataTranslator = dataTranslator

    override def getQueryTranslator: MorphBaseQueryTranslator = queryTranslator

    override def getQueryProcessor: MorphBaseQueryProcessor = queryProcessor

    val logger = Logger.getLogger(this.getClass());

    def createConnection: GenericConnection

    def createUnfolder: MorphBaseUnfolder

    def createDataSourceReader: MorphBaseDataSourceReader

    def createDataTranslator: MorphBaseDataTranslator

    def createQueryTranslator: MorphBaseQueryTranslator

    def createQueryProcessor: MorphBaseQueryProcessor;

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

    /**
     * Optional database-specific steps of the factory creation
     */
    def postCreateFactory: Unit

    def createRunner: MorphBaseRunner = {
        new MorphBaseRunner(this)
    }

}

object MorphBaseRunnerFactory {

    var properties: MorphProperties = null

    var mappingDocument: R2RMLMappingDocument = null

    /**
     * Initialize the factory: create global objects that can be shared by parallel executions
     * of a runner, i.e. properties and mapping document.
     *
     * This method must be called before create any factory.
     */
    def initFactory(props: MorphProperties) = {
        MorphBaseRunnerFactory.properties = props
        MorphBaseRunnerFactory.mappingDocument = R2RMLMappingDocument(properties)
    }

    /**
     * Create all instances needed to safely execute several runners in parallel
     *
     * @Note initFactory() method must have been called before calling createFactory()
     */
    def createFactory: MorphBaseRunnerFactory = {

        val factory = Class.forName(properties.runnerFactoryClassName).newInstance().asInstanceOf[MorphBaseRunnerFactory]

        factory.properties = MorphBaseRunnerFactory.properties
        factory.mappingDocument = MorphBaseRunnerFactory.mappingDocument

        factory.connection = factory.createConnection
        factory.unfolder = factory.createUnfolder
        factory.dataSourceReader = factory.createDataSourceReader
        factory.materializer = factory.createMaterializer
        factory.dataTranslator = factory.createDataTranslator
        factory.queryTranslator = factory.createQueryTranslator
        factory.queryProcessor = factory.createQueryProcessor

        factory.postCreateFactory // optionally perform any other database-specific step of the creation
        factory
    }
}
