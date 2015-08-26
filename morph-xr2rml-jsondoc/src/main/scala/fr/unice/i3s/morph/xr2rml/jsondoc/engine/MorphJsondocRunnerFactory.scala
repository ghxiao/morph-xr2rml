package fr.unice.i3s.morph.xr2rml.jsondoc.engine

import java.io.Writer

import org.apache.log4j.Logger

import es.upm.fi.dia.oeg.morph.base.Constants
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
import fr.unice.i3s.morph.xr2rml.jsondoc.mongo.MongoUtils

class MorphJsondocRunnerFactory extends MorphBaseRunnerFactory {

    override val logger = Logger.getLogger(this.getClass().getName())

    override def createRunner(
        mappingDocument: R2RMLMappingDocument,
        unfolder: MorphBaseUnfolder,
        dataTranslator: Option[MorphBaseDataTranslator],
        queryTranslator: Option[IQueryTranslator],
        resultProcessor: Option[AbstractQueryResultTranslator],
        outputStream: Writer): MorphBaseRunner = {

        new MorphBaseRunner(
            mappingDocument.asInstanceOf[R2RMLMappingDocument],
            unfolder.asInstanceOf[MorphJsondocUnfolder],
            dataTranslator.asInstanceOf[Option[MorphJsondocDataTranslator]],
            queryTranslator,
            resultProcessor,
            outputStream)
    }

    override def readMappingDocumentFile(mappingDocumentFile: String, props: MorphProperties, connection: GenericConnection): R2RMLMappingDocument = {
        val mappingDocument = R2RMLMappingDocument(mappingDocumentFile, props, connection);
        mappingDocument
    }

    override def createUnfolder(md: R2RMLMappingDocument, props: MorphProperties): MorphJsondocUnfolder = {
        val unfolder = new MorphJsondocUnfolder(md.asInstanceOf[R2RMLMappingDocument], props);
        unfolder.dbType = props.databaseType;
        unfolder;
    }

    override def createDataTranslator(
        mappingDocument: R2RMLMappingDocument,
        materializer: MorphBaseMaterializer,
        unfolder: MorphBaseUnfolder,
        dataSourceReader: MorphBaseDataSourceReader,
        connection: GenericConnection,
        properties: MorphProperties): MorphBaseDataTranslator = {
        new MorphJsondocDataTranslator(
            mappingDocument.asInstanceOf[R2RMLMappingDocument],
            materializer,
            unfolder.asInstanceOf[MorphJsondocUnfolder],
            null, // dataSourceReader.asInstanceOf[MorphJsondocDataSourceReader]: unused in data materialization
            connection, properties);
    }

    /**
     * Return a valid connection to the database or raises a run time exception if anything goes wrong
     */
    override def createConnection(configurationProperties: MorphProperties): GenericConnection = {

        if (configurationProperties.noOfDatabase == 0)
            throw new Exception("No database connection parameters found in the configuration.")

        val dbType = configurationProperties.databaseType
        val cnx = dbType match {
            case Constants.DATABASE_MONGODB =>
                MongoUtils.createConnection(configurationProperties)
            case _ =>
                throw new Exception("Database type not supported: " + dbType)
        }
        cnx
    }
}
