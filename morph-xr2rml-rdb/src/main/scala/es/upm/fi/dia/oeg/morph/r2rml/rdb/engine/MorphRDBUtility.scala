package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.util.Collection
import es.upm.fi.dia.oeg.morph.base.Constants
import Zql.ZExpression
import scala.collection.JavaConversions._
import Zql.ZConstant
import Zql.ZQuery
import java.io.ByteArrayInputStream
import Zql.ZqlParser
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import es.upm.fi.dia.oeg.morph.base.sql.SQLDataType
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.sql.SQLQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap

class MorphRDBUtility {

}

object MorphRDBUtility {
    val logger = Logger.getLogger(this.getClass().getName());

    def generateCondForWellDefinedURI(termMap: R2RMLTermMap, ownerTriplesMap: MorphBaseClassMapping, uri: String, alias: String): ZExpression = {
        val logicalTable = ownerTriplesMap.getLogicalSource();
        val logicalTableMetaData = logicalTable.tableMetaData;
        val dbType = if (logicalTableMetaData.isDefined) { logicalTableMetaData.get.dbType }
        else { Constants.DATABASE_DEFAULT }

        val result: ZExpression = {
            if (termMap.termMapType == Constants.MorphTermMapType.TemplateTermMap) {
                val matchedColValues = termMap.getTemplateValues(uri);
                if (matchedColValues == null || matchedColValues.size == 0) {
                    val errorMessage = "uri " + uri + " doesn't match the template : " + termMap.templateString;
                    logger.debug(errorMessage);
                    null
                } else {
                    val exprs: List[ZExpression] = {
                        val exprsAux = matchedColValues.keySet.map(pkColumnString => {
                            val value = matchedColValues(pkColumnString);

                            val columnTypeName = null

                            val pkColumnConstant = MorphSQLConstant.apply(alias + "." + pkColumnString, ZConstant.COLUMNNAME, dbType);

                            val pkValueConstant = new ZConstant(value, ZConstant.STRING)

                            val expr = new ZExpression("=", pkColumnConstant, pkValueConstant);
                            expr;
                        })
                        exprsAux.toList;
                    }

                    MorphSQLUtility.combineExpresions(
                        exprs, Constants.SQL_LOGICAL_OPERATOR_AND);
                }
            } else {
                null
            }
        }

        logger.debug("generateCondForWellDefinedURI = " + result);
        result;
    }

    def toZQuery(sqlString: String): ZQuery = {
        try {
            val bs = new ByteArrayInputStream(sqlString.getBytes());
            val parser = new ZqlParser(bs);
            val statement = parser.readStatement();
            val zQuery = statement.asInstanceOf[ZQuery];
            zQuery;
        } catch {
            case e: Exception => {
                val errorMessage = "error parsing query string : \n" + sqlString;
                logger.error(errorMessage);
                logger.error("error message = " + e.getMessage());
                throw e;
            }
            case e: Error => {
                val errorMessage = "error parsing query string : \n" + sqlString;
                logger.error(errorMessage);
                throw new Exception(errorMessage);
            }
        }
    }

    def toSQLQuery(sqlString: String): SQLQuery = {
        val zQuery = this.toZQuery(sqlString);
        val sqlQuery = new SQLQuery(zQuery);
        sqlQuery;
    }
}