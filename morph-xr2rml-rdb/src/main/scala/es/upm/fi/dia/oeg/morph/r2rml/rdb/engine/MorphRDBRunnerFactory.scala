package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.io.Writer
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslatorFactory
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseQueryResultWriter
import es.upm.fi.dia.oeg.morph.base.engine.QueryResultWriterFactory
import es.upm.fi.dia.oeg.morph.base.engine.QueryTranslationOptimizer
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslatorFactory
import es.upm.fi.dia.oeg.morph.base.querytranslator.engine.MorphXMLQueryResultWriter
import es.upm.fi.dia.oeg.morph.base.querytranslator.engine.QueryResultTranslator

class MorphRDBRunnerFactory extends MorphBaseRunnerFactory {

    override def createConnection(configurationProperties: MorphProperties): GenericConnection = {
        val connection = if (configurationProperties.noOfDatabase > 0) {
            val databaseUser = configurationProperties.databaseUser;
            val databaseName = configurationProperties.databaseName;
            val databasePassword = configurationProperties.databasePassword;
            val databaseDriver = configurationProperties.databaseDriver;
            val databaseURL = configurationProperties.databaseURL;
            DBUtility.getLocalConnection(databaseUser, databaseName, databasePassword, databaseDriver, databaseURL, "Runner");
        } else
            null

        new GenericConnection(Constants.DatabaseType.Relational, connection)
    }

    override def createUnfolder(props: MorphProperties, md: R2RMLMappingDocument): MorphRDBUnfolder = {
        val unfolder = new MorphRDBUnfolder(md, props);
        unfolder.dbType = props.databaseType;
        unfolder;
    }

    override def createDataSourceReader(properties: MorphProperties, connection: GenericConnection): MorphRDBDataSourceReader = {
        val reader = new MorphRDBDataSourceReader()
        reader.setConnection(connection);
        reader.setTimeout(properties.databaseTimeout)
        reader
    }

    override def createDataTranslator(
        mappingDocument: R2RMLMappingDocument,
        materializer: MorphBaseMaterializer,
        unfolder: MorphBaseUnfolder,
        dataSourceReader: MorphBaseDataSourceReader,
        connection: GenericConnection,
        properties: MorphProperties): MorphBaseDataTranslator = {

        new MorphRDBDataTranslator(
            mappingDocument.asInstanceOf[R2RMLMappingDocument],
            materializer,
            unfolder.asInstanceOf[MorphRDBUnfolder],
            dataSourceReader.asInstanceOf[MorphRDBDataSourceReader],
            connection, properties);
    }

    override def createQueryTranslator(properties: MorphProperties, md: R2RMLMappingDocument, connection: GenericConnection): IQueryTranslator = {

        val queryTranslatorFactoryClassName = properties.queryTranslatorFactoryClassName;
        val className =
            if (queryTranslatorFactoryClassName == null || queryTranslatorFactoryClassName.equals(""))
                Constants.QUERY_TRANSLATOR_FACTORY_CLASSNAME_DEFAULT;
            else
                queryTranslatorFactoryClassName;

        val queryOptimizer = new QueryTranslationOptimizer()
        queryOptimizer.selfJoinElimination = properties.selfJoinElimination;
        queryOptimizer.subQueryElimination = properties.subQueryElimination;
        queryOptimizer.transJoinSubQueryElimination = properties.transJoinSubQueryElimination;
        queryOptimizer.transSTGSubQueryElimination = properties.transSTGSubQueryElimination;
        queryOptimizer.subQueryAsView = properties.subQueryAsView;

        val queryTranslatorFactory = Class.forName(className).newInstance().asInstanceOf[IQueryTranslatorFactory];
        val queryTranslator = queryTranslatorFactory.createQueryTranslator(md, connection, properties);
        queryTranslator.properties = properties;
        queryTranslator.optimizer = queryOptimizer;

        logger.info("query translator = " + queryTranslator);
        queryTranslator
    }

    override def createQueryResultTranslator(
        properties: MorphProperties,
        md: R2RMLMappingDocument,
        connection: GenericConnection,
        dataSourceReader: MorphBaseDataSourceReader,
        queryTranslator: Option[IQueryTranslator],
        outputStream: Writer): Option[AbstractQueryResultTranslator] = {

        // Building QUERY RESULT WRITER
        val queryResultWriter = if (queryTranslator.isDefined)
            Some(new MorphXMLQueryResultWriter(queryTranslator.get, outputStream))
        else None

        // Building RESULT PROCESSOR
        val resultProcessor = if (queryResultWriter.isDefined)
            Some(new QueryResultTranslator(dataSourceReader, queryResultWriter.get))
        else None
        resultProcessor
    }
}
