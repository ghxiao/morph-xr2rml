package es.upm.fi.dia.oeg.morph.base.engine

import java.io.FileWriter
import java.io.StringWriter
import java.io.Writer
import org.apache.log4j.Logger
import com.hp.hpl.jena.query.QueryFactory
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.materializer.MaterializerFactory
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import java.io.OutputStreamWriter
import java.io.FileOutputStream
import java.io.PrintWriter

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
        val mappingDocument = this.readMappingDocumentFile(properties.mappingDocumentFilePath, properties, connection);

        // Building UNFOLDER
        val unfolder = this.createUnfolder(mappingDocument, properties);

        val outputStream: Writer = if (properties.outputFilePath.isDefined) {
            //new OutputStreamWriter(new FileOutputStream(properties.outputFilePath.get), "UTF-8")
            new PrintWriter(properties.outputFilePath.get, "UTF-8")
        } else { new StringWriter }

        // Building MATERIALIZER
        val materializer = this.buildMaterializer(properties, mappingDocument, outputStream);

        // Building DATA SOURCE READER (query rewriting mode only)
        val dataSourceReader = MorphBaseDataSourceReader(properties.queryEvaluatorClassName, connection, properties.databaseTimeout);

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
        // The Query Translator, Query Result Writer, Result Processor, as well as the 
        // Data Source Reader are only applicable in the case of query rewriting access mode, 
        // i.e. not in data materialization.
        // ---------------------------------------------------------------------------------

        // Building QUERY TRANSLATOR
        logger.info("Building query translator...");
        val queryTranslatorFactoryClassName = properties.queryTranslatorFactoryClassName;
        val queryTranslator =
            if (queryTranslatorFactoryClassName == null)
                None
            else
                try {
                    val qtAux = this.buildQueryTranslator(queryTranslatorFactoryClassName, mappingDocument, connection, properties);
                    Some(qtAux);
                } catch {
                    case e: Exception => {
                        logger.warn("Error building query translator: " + e.getMessage());
                        e.printStackTrace()
                        None
                    }
                }

        // Building QUERY RESULT WRITER
        val queryResultWriter = if (queryTranslator.isDefined) {
            val queryResultWriterFactoryClassName = properties.queryResultWriterFactoryClassName;
            val qrwAux = this.buildQueryResultWriter(queryResultWriterFactoryClassName, queryTranslator.get, outputStream);
            Some(qrwAux)
        } else { None }

        // Building RESULT PROCESSOR
        val resultProcessor = if (queryResultWriter.isDefined) {
            val resultProcessorAux = this.buildQueryResultTranslator(dataSourceReader, queryResultWriter.get);
            Some(resultProcessorAux)
        } else { None }

        // Creation of final runner object using the concrete class instance
        val runner = this.createRunner(mappingDocument, unfolder, dataTranslator, queryTranslator, resultProcessor, outputStream)

        runner.ontologyFilePath = properties.ontologyFilePath;
        if (properties.queryFilePath.isDefined) {
            runner.sparqlQuery = Some(QueryFactory.read(properties.queryFilePath.get))
        }

        runner;
    }

    def createRunner(mappingDocument: MorphBaseMappingDocument,
                     unfolder: MorphBaseUnfolder,
                     dataTranslator: Option[MorphBaseDataTranslator],
                     queryTranslator: Option[IQueryTranslator],
                     resultProcessor: Option[AbstractQueryResultTranslator],
                     outputStream: Writer): MorphBaseRunner;

    def readMappingDocumentFile(mappingDocumentFile: String, props: MorphProperties, connection: GenericConnection): MorphBaseMappingDocument;

    def createUnfolder(md: MorphBaseMappingDocument, properties: MorphProperties): MorphBaseUnfolder;

    def createDataTranslator(md: MorphBaseMappingDocument, materializer: MorphBaseMaterializer, unfolder: MorphBaseUnfolder, dataSourceReader: MorphBaseDataSourceReader, connection: GenericConnection, properties: MorphProperties): MorphBaseDataTranslator;

    def createConnection(configurationProperties: MorphProperties): GenericConnection;

    private def buildQueryTranslator(queryTranslatorFactoryClassName: String, md: MorphBaseMappingDocument, connection: GenericConnection, properties: MorphProperties): IQueryTranslator = {
        val className =
            if (queryTranslatorFactoryClassName == null || queryTranslatorFactoryClassName.equals(""))
                Constants.QUERY_TRANSLATOR_FACTORY_CLASSNAME_DEFAULT;
            else
                queryTranslatorFactoryClassName;

        val queryTranslatorFactory = Class.forName(className).newInstance().asInstanceOf[IQueryTranslatorFactory];
        val queryTranslator = queryTranslatorFactory.createQueryTranslator(md, connection, properties);

        //query translation optimizer
        val queryTranslationOptimizer = this.buildQueryTranslationOptimizer();
        val eliminateSelfJoin = properties.selfJoinElimination;
        queryTranslationOptimizer.selfJoinElimination = eliminateSelfJoin;
        val eliminateSubQuery = properties.subQueryElimination;
        queryTranslationOptimizer.subQueryElimination = eliminateSubQuery;
        val transJoinEliminateSubQuery = properties.transJoinSubQueryElimination;
        queryTranslationOptimizer.transJoinSubQueryElimination = transJoinEliminateSubQuery;
        val transSTGEliminateSubQuery = properties.transSTGSubQueryElimination;
        queryTranslationOptimizer.transSTGSubQueryElimination = transSTGEliminateSubQuery;
        val subQueryAsView = properties.subQueryAsView;
        queryTranslationOptimizer.subQueryAsView = subQueryAsView;
        queryTranslator.optimizer = queryTranslationOptimizer;
        logger.info("query translator = " + queryTranslator);

        //sparql query
        val queryFilePath = properties.queryFilePath;
        //		queryTranslator.setSPARQLQueryByFile(queryFilePath);

        queryTranslator.properties = properties;
        queryTranslator
    }

    private def buildQueryResultWriter(queryResultWriterFactoryClassName: String, queryTranslator: IQueryTranslator, pOutputStream: Writer): MorphBaseQueryResultWriter = {
        val className = if (queryResultWriterFactoryClassName == null
            || queryResultWriterFactoryClassName.equals("")) {
            Constants.QUERY_RESULT_WRITER_FACTORY_CLASSNAME_DEFAULT;
        } else {
            queryResultWriterFactoryClassName;
        }

        val queryResultWriterFactory = Class.forName(className).newInstance().asInstanceOf[QueryResultWriterFactory];
        val queryResultWriter = queryResultWriterFactory.createQueryResultWriter(
            queryTranslator, pOutputStream);
        logger.info("query result writer = " + queryResultWriter);
        queryResultWriter
    }

    private def buildQueryTranslationOptimizer(): QueryTranslationOptimizer = {
        new QueryTranslationOptimizer();
    }

    private def buildQueryResultTranslator(dataSourceReader: MorphBaseDataSourceReader, queryResultWriter: MorphBaseQueryResultWriter): AbstractQueryResultTranslator = {
        val className = Constants.QUERY_RESULT_TRANSLATOR_CLASSNAME_DEFAULT;

        val queryResultTranslatorFactory = Class.forName(className).newInstance().asInstanceOf[AbstractQueryResultTranslatorFactory];
        val queryResultTranslator = queryResultTranslatorFactory.createQueryResultTranslator(
            dataSourceReader, queryResultWriter);
        queryResultTranslator;

    }

    private def buildMaterializer(configurationProperties: MorphProperties, mappingDocument: MorphBaseMappingDocument, outputStream: Writer): MorphBaseMaterializer = {
        val jenaMode = configurationProperties.jenaMode;
        val materializer = MaterializerFactory.create(outputStream, jenaMode);
        val mappingDocumentPrefixMap = mappingDocument.mappingDocumentPrefixMap;
        if (mappingDocumentPrefixMap != null) {
            materializer.setModelPrefixMap(mappingDocumentPrefixMap);
        }
        materializer
    }
}

