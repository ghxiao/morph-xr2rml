package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.io.Writer
import java.sql.Connection

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.querytranslator.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryResultProcessor
import es.upm.fi.dia.oeg.morph.base.querytranslator.NameGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.QueryTranslationOptimizer
import es.upm.fi.dia.oeg.morph.base.querytranslator.engine.MorphXMLQueryResultProcessor
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBAlphaGenerator
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBBetaGenerator
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBCondSQLGenerator
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBPRSQLGenerator
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBQueryTranslator

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
        connection: GenericConnection,
        properties: MorphProperties): MorphBaseDataTranslator = {

        new MorphRDBDataTranslator(
            mappingDocument, materializer, unfolder.asInstanceOf[MorphRDBUnfolder], connection, properties);
    }

    override def createQueryTranslator(properties: MorphProperties, md: R2RMLMappingDocument, connection: GenericConnection): IQueryTranslator = {

        val queryOptimizer = new QueryTranslationOptimizer()
        queryOptimizer.selfJoinElimination = properties.selfJoinElimination;
        queryOptimizer.subQueryElimination = properties.subQueryElimination;
        queryOptimizer.transJoinSubQueryElimination = properties.transJoinSubQueryElimination;
        queryOptimizer.transSTGSubQueryElimination = properties.transSTGSubQueryElimination;
        queryOptimizer.subQueryAsView = properties.subQueryAsView;

        val queryTranslator = createQueryTranslator(md, connection, properties);
        queryTranslator.properties = properties;
        queryTranslator.optimizer = queryOptimizer;

        logger.info("query translator = " + queryTranslator);
        queryTranslator
    }

    private def createQueryTranslator(abstractMappingDocument: R2RMLMappingDocument, conn: GenericConnection, properties: MorphProperties): IQueryTranslator = {
        val md = abstractMappingDocument.asInstanceOf[R2RMLMappingDocument];
        val unfolder = new MorphRDBUnfolder(md, properties);

        val queryTranslator = new MorphRDBQueryTranslator(
            new NameGenerator(),
            new MorphRDBAlphaGenerator(md, unfolder),
            new MorphRDBBetaGenerator(md, unfolder),
            new MorphRDBCondSQLGenerator(md, unfolder),
            new MorphRDBPRSQLGenerator(md, unfolder));

        if (conn != null) {
            if (!conn.isRelationalDB)
                throw new Exception("Invalid connection type: should be a relational db connection")
            queryTranslator.connection = conn.concreteCnx.asInstanceOf[Connection];
        }
        queryTranslator.mappingDocument = md;
        queryTranslator;
    }

    override def createQueryResultProcessor(
        properties: MorphProperties,
        md: R2RMLMappingDocument,
        connection: GenericConnection,
        dataSourceReader: MorphBaseDataSourceReader,
        queryTranslator: IQueryTranslator,
        outputStream: Writer): MorphBaseQueryResultProcessor = {

        val queryResultProc = new MorphXMLQueryResultProcessor(md, properties, outputStream, dataSourceReader)
        queryResultProc.projectionGenerator = queryTranslator.getPRSQLGen
        queryResultProc
    }
}
