package es.upm.fi.dia.oeg.morph.base.engine

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GenericConnection

/**
 * A data source reader is used in case of the query rewriting access method,
 * to execute queries on the fly.
 * It is not used in the data materialization access method.
 */
abstract class MorphBaseDataSourceReader {
    def execute(query: String): MorphBaseResultSet;
    def setConnection(obj: GenericConnection);
    def setTimeout(timeout: Int);
    def closeConnection();
}

object MorphBaseDataSourceReader {

    def apply(dataSourceReaderClassName: String, connection: GenericConnection, timeout: Int): MorphBaseDataSourceReader = {
        val className = if (dataSourceReaderClassName == null || dataSourceReaderClassName.equals("")) {
            Constants.DATASOURCE_READER_CLASSNAME_DEFAULT;
        } else {
            dataSourceReaderClassName;
        }

        val classInstance = Class.forName(className).newInstance()
        val dataSourceReader = classInstance.asInstanceOf[MorphBaseDataSourceReader];
        dataSourceReader.setConnection(connection);
        dataSourceReader.setTimeout(timeout)

        dataSourceReader
    }
}