package es.upm.fi.dia.oeg.morph.base.sql

import org.apache.log4j.Logger

class MorphColumnMetaData(val tableName: String, val columnName: String, val dataType: String, val isNullable: Boolean, val characterMaximumLength: Long, val columnKey: Option[String]) {

    val logger = Logger.getLogger(this.getClass().getName());
    logger.trace("\t\tColumn MetaData created: " + this.tableName + "." + this.columnName);

    def isPrimaryKeyColumn = {
        this.columnKey.isDefined && this.columnKey.get.equals("PRI");
    }
}