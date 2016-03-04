package fr.unice.i3s.morph.xr2rml.mongo.engine

import java.io.Writer
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.IMorphFactory
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryResultProcessor
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphMongoQueryResultProcessor
import fr.unice.i3s.morph.xr2rml.mongo.querytranslator.MorphMongoQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseTriplePatternBinder

class MorphMongoRunnerFactory extends MorphBaseRunnerFactory {

    override val logger = Logger.getLogger(this.getClass().getName())

    /**
     * Return a valid connection to the database or raises a runtime exception if anything goes wrong
     */
    override def createConnection: GenericConnection = {
        val props = this.getProperties
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

    override def createUnfolder: MorphMongoUnfolder = {
        new MorphMongoUnfolder(this)
    }

    override def createDataSourceReader: MorphBaseDataSourceReader = {
        new MorphMongoDataSourceReader(this)
    }

    override def createDataTranslator: MorphBaseDataTranslator = {
        new MorphMongoDataTranslator(this);
    }

    override def createQueryTranslator: MorphBaseQueryTranslator = {
        new MorphMongoQueryTranslator(this)
    }

    override def createQueryResultProcessor: MorphBaseQueryResultProcessor = {
        new MorphMongoQueryResultProcessor(this)
    }
}
