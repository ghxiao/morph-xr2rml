package es.upm.fi.dia.oeg.morph.rdb.engine

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
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryResultProcessor
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBAlphaGenerator
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBBetaGenerator
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBCondSQLGenerator
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBPRSQLGenerator
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBQueryOptimizer
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBQueryResultProcessor
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IMorphFactory

class MorphRDBRunnerFactory extends MorphBaseRunnerFactory {

    override def createConnection: GenericConnection = {
        val properties = this.getProperties
        val connection = if (properties.noOfDatabase > 0) {
            val databaseUser = properties.databaseUser;
            val databaseName = properties.databaseName;
            val databasePassword = properties.databasePassword;
            val databaseDriver = properties.databaseDriver;
            val databaseURL = properties.databaseURL;
            DBUtility.getLocalConnection(databaseUser, databaseName, databasePassword, databaseDriver, databaseURL, "Runner");
        } else
            null

        new GenericConnection(Constants.DatabaseType.Relational, connection)
    }

    override def createUnfolder: MorphRDBUnfolder = {
        new MorphRDBUnfolder(this)
    }

    override def createDataSourceReader: MorphRDBDataSourceReader = {
        new MorphRDBDataSourceReader(this)
    }

    override def createDataTranslator: MorphBaseDataTranslator = {
        new MorphRDBDataTranslator(this);
    }

    override def createQueryTranslator: MorphBaseQueryTranslator = {
        new MorphRDBQueryTranslator(this)
    }

    override def createQueryResultProcessor: MorphBaseQueryResultProcessor = {
        new MorphRDBQueryResultProcessor(this)
    }
}
