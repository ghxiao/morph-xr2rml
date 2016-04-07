package es.upm.fi.dia.oeg.morph.rdb.engine

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.GenericConnection
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryProcessor
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBQueryProcessor
import es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphRDBQueryTranslator

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

    override def createQueryProcessor: MorphBaseQueryProcessor = {
        new MorphRDBQueryProcessor(this)
    }
}
