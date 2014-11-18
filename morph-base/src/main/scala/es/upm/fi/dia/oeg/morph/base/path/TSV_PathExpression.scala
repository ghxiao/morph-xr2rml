package es.upm.fi.dia.oeg.morph.base.path

import java.io.Reader
import java.io.StringReader

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVRecord
import org.apache.log4j.Logger

import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class TSV_PathExpression(
    pathExpression: String)
        extends PathExpression(pathExpression) {

    private val parser = CSVFormat.TDF.withIgnoreSurroundingSpaces(true).withIgnoreEmptyLines(true)

    val logger = Logger.getLogger(this.getClass().getName())

    override def toString: String = { "TSV: " + pathExpression + ". Quote: " + parser.getQuoteCharacter() }

    /**
     * Return the TSV elements corresponding to the column named by pathExpression.
     * One element is returned for each row of the input data.
     * In case the column name is false (out of bound, string and not integer), an empty string is returned.
     */
    def evaluate(value: String): List[Object] = {

        val isInteger: Boolean = try {
            Integer.parseInt(pathExpression)
            true
        } catch {
            case e: Exception => false
        }

        try {
            val valRedear: Reader = new StringReader(value)
            val result = for (record: CSVRecord <- parser.parse(valRedear).getRecords()) yield {
                if (isInteger)
                    record.get(Integer.parseInt(pathExpression))
                else
                    record.get(pathExpression)
            }
            result.toList
        } catch {
            case e: Exception => {
                logger.error("Unable to get TSV element with column " + pathExpression + ". Returning empty string.")
                List("")
            }
        }
    }
}

object TSV_PathExpression {

    /**
     * Make an instance from a path construct expression like CSV(expr)
     */
    def parse(pathConstructExpr: String): CSV_PathExpression = {

        // Remove the path constructor name "TSV(" and the final ")"
        var expr = pathConstructExpr.trim().substring(xR2RML_Constants.xR2RML_PATH_CONSTR_TSV.length + 1, pathConstructExpr.length - 1)

        new CSV_PathExpression(MixedSyntaxPath.unescapeChars(expr))
    }
}    
