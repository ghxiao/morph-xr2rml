package fr.unice.i3s.morph.xr2rml.jsondoc.engine

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.QueryTranslationOptimizerFactory
import java.io.OutputStream
import java.io.Writer
import es.upm.fi.dia.oeg.morph.base.DBUtility

class MorphJsondocRunnerFactory extends MorphBaseRunnerFactory {

    override def createRunner(
        mappingDocument: MorphBaseMappingDocument,
        unfolder: MorphBaseUnfolder,
        dataTranslator: Option[MorphBaseDataTranslator],
        queryTranslator: Option[IQueryTranslator],
        resultProcessor: Option[AbstractQueryResultTranslator],
        outputStream: Writer): MorphJsondocRunner = {

        new MorphJsondocRunner(
            mappingDocument.asInstanceOf[R2RMLMappingDocument],
            unfolder.asInstanceOf[MorphJsondocUnfolder],
            dataTranslator.asInstanceOf[Option[MorphJsondocDataTranslator]],
            queryTranslator,
            resultProcessor,
            outputStream)
    }

    override def readMappingDocumentFile(mappingDocumentFile: String, props: MorphProperties, connection: Connection): MorphBaseMappingDocument = {
        val mappingDocument = R2RMLMappingDocument(mappingDocumentFile, props, connection);
        mappingDocument
    }

    override def createUnfolder(md: MorphBaseMappingDocument, props: MorphProperties): MorphJsondocUnfolder = {
        val unfolder = new MorphJsondocUnfolder(md.asInstanceOf[R2RMLMappingDocument], props);
        unfolder.dbType = props.databaseType;
        unfolder;
    }

    override def createDataTranslator(
        mappingDocument: MorphBaseMappingDocument,
        materializer: MorphBaseMaterializer,
        unfolder: MorphBaseUnfolder,
        dataSourceReader: MorphBaseDataSourceReader,
        connection: Connection,
        properties: MorphProperties): MorphBaseDataTranslator = {
        new MorphJsondocDataTranslator(
            mappingDocument.asInstanceOf[R2RMLMappingDocument],
            materializer,
            unfolder.asInstanceOf[MorphJsondocUnfolder],
            null, // dataSourceReader.asInstanceOf[MorphJsondocDataSourceReader]: unused in data materialization
            connection, properties);
    }

    override def createConnection(configurationProperties: MorphProperties): Connection = {
        val connection = if (configurationProperties.noOfDatabase > 0) {
            val databaseUser = configurationProperties.databaseUser;
            val databaseName = configurationProperties.databaseName;
            val databasePassword = configurationProperties.databasePassword;
            val databaseDriver = configurationProperties.databaseDriver;
            val databaseURL = configurationProperties.databaseURL;
            DBUtility.getLocalConnection(databaseUser, databaseName, databasePassword, databaseDriver, databaseURL, "Runner");
        } else {
            null
        }

        connection;
    }
}

object MorphRDBRunnerFactory {

}