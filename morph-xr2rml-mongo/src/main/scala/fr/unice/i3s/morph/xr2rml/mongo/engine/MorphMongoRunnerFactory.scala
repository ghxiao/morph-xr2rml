package fr.unice.i3s.morph.xr2rml.mongo.engine

import java.io.Writer

import org.apache.log4j.Logger

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryResultProcessor
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphMongoQueryResultProcessor
import fr.unice.i3s.morph.xr2rml.mongo.querytranslator.MorphMongoQueryTranslator

class MorphMongoRunnerFactory extends MorphBaseRunnerFactory {

    override val logger = Logger.getLogger(this.getClass().getName())

    /**
     * Return a valid connection to the database or raises a runtime exception if anything goes wrong
     */
    override def createConnection(props: MorphProperties): GenericConnection = {
        if (props.noOfDatabase == 0)
            throw new Exception("No database connection parameters found in the configuration.")

        val dbType = props.databaseType
        val cnx = dbType match {
            case Constants.DATABASE_MONGODB =>
                MorphMongoDataSourceReader.createConnection(props)
            case _ =>
                throw new Exception("Database type not supported: " + dbType)
        }
        cnx
    }

    override def createUnfolder(props: MorphProperties, md: R2RMLMappingDocument): MorphMongoUnfolder = {
        val dbType = props.databaseType
        val unfolder = new MorphMongoUnfolder(md, props)
        unfolder.dbType = dbType
        unfolder
    }

    override def createDataSourceReader(
        properties: MorphProperties, connection: GenericConnection): MorphBaseDataSourceReader = {
        val reader = new MorphMongoDataSourceReader()
        reader.setConnection(connection);
        reader
    }

    override def createDataTranslator(
        mappingDocument: R2RMLMappingDocument,
        materializer: MorphBaseMaterializer,
        unfolder: MorphBaseUnfolder,
        dataSourceReader: MorphBaseDataSourceReader,
        properties: MorphProperties): MorphBaseDataTranslator = {

        new MorphMongoDataTranslator(
            mappingDocument, materializer, unfolder.asInstanceOf[MorphMongoUnfolder], dataSourceReader, properties);
    }

    override def createQueryTranslator(
        properties: MorphProperties, md: R2RMLMappingDocument, dataSourceReader: MorphBaseDataSourceReader): MorphBaseQueryTranslator = {
        new MorphMongoQueryTranslator(md)
    }

    override def createQueryResultProcessor(
        properties: MorphProperties,
        md: R2RMLMappingDocument,
        dataSourceReader: MorphBaseDataSourceReader,
        dataTranslator: MorphBaseDataTranslator,
        queryTranslator: MorphBaseQueryTranslator,
        outputStream: Writer): MorphBaseQueryResultProcessor = {
        new MorphMongoQueryResultProcessor(md, properties, dataSourceReader, dataTranslator.asInstanceOf[MorphMongoDataTranslator], outputStream)
    }
}
