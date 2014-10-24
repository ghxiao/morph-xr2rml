package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import org.apache.log4j.Logger
import java.util.Collection
import es.upm.fi.dia.oeg.morph.base.DBUtility
import java.sql.ResultSet
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import java.sql.ResultSetMetaData
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.RegexUtility
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import Zql.ZConstant
import es.upm.fi.dia.oeg.morph.base.sql.DatatypeMapper
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLLogicalTable
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.AnonId
import com.hp.hpl.jena.vocabulary.RDF
import com.hp.hpl.jena.rdf.model.Literal
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants
import com.hp.hpl.jena.rdf.model.RDFList
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.ModelFactoryBase
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateMap
import java.io.BufferedWriter
import java.io.FileWriter
import javax.json.Json
import java.io.FileReader
import javax.json.JsonReader
import es.upm.fi.dia.oeg.morph.r2rml.model.xR2RMLRecursiveParse
import javax.json.JsonValue
import javax.json.JsonObject
import javax.json.JsonString
import javax.json.JsonNumber
import javax.json.JsonArray
import java.io.File
import java.io.PrintStream
import java.io.IOException
import java.io.PrintWriter
import java.io.BufferedReader
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTable
import javax.json.JsonStructure
import es.upm.fi.dia.oeg.morph.base.xR2RML_CSV_to_JSON
import es.upm.fi.dia.oeg.morph.base.xR2RML_XML_to_JSON

object xR2RMLDataTranslator {

    val logger = Logger.getLogger(this.getClass().getName());

    // As the joinParse properties cannot be added in the "select" request, this function checks the join conditions

    def checkJoinParseCondition(refObjectMap: R2RMLRefObjectMap, rsn: ResultSet, databaseType: String, parentAlias: String, childAlias: String): Boolean = {
        var result = true;

        for (join <- refObjectMap.getJoinConditions) {
            if (result) {
                val dbType = databaseType;
                val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
                var columnterm = childAlias + "_" + join.childColumnName;

                var childValue = this.getResultSetValueforJoinParse(rsn, columnterm, dbType);
                columnterm = parentAlias + "_" + join.parentColumnName;

                var parentValue = this.getResultSetValueforJoinParse(rsn, columnterm, dbType);

                var childparsetype = join.childParse.getParseType.get
                var parentparsetype = join.parentParse.getParseType.get

                if (childparsetype.equals(Constants.R2RML_LITERAL_URI) && parentparsetype.equals(Constants.R2RML_LITERAL_URI)) {
                    if (!childValue.equals(parentValue)) { result = false }
                } else if (childparsetype.equals(xR2RML_Constants.xR2RML_RRXLISTORMAP_URI) && parentparsetype.equals(xR2RML_Constants.xR2RML_RRXLISTORMAP_URI)) {

                    var ChildList = Array.ofDim[String](0);
                    var ParentList = Array.ofDim[String](0);

                    var out = new PrintWriter(new BufferedWriter(new FileWriter(xR2RML_Constants.utilFilesave, false)))
                    out.println(childValue)
                    out.flush()
                    out.close()

                    if (join.childParse.getJoinParseFormat.equals(xR2RML_Constants.xR2RML_JSON_URI)) {
                        // in case the child is a json data
                        try {
                            var jsonR = Json.createReader(new FileReader(xR2RML_Constants.utilFilesave))
                            var jsonData = jsonR.read()
                            jsonR.close();

                            ChildList = jsonData.getValueType match {
                                case JsonValue.ValueType.OBJECT => {
                                    var obj = jsonData.asInstanceOf[JsonObject]
                                    var n = 0
                                    for (name <- obj.keySet) { n = n + 1 }
                                    var tab = Array.ofDim[String](n);
                                    n = 0
                                    for (name <- obj.keySet) { tab(n) = obj.get(name).toString(); n = n + 1 }
                                    tab
                                }
                                case JsonValue.ValueType.ARRAY => {
                                    var obj = jsonData.asInstanceOf[JsonArray]
                                    var n = 0
                                    for (name <- obj) { n = n + 1 }
                                    var tab = Array.ofDim[String](n);
                                    n = 0
                                    for (name <- obj) { tab(n) = name.toString(); n = n + 1 }
                                    tab
                                }
                                case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                                    logger.info("the data " + jsonData.toString() + "is not a list")
                                    System.exit(0)
                                    null
                                }
                                case _ => null
                            };

                        } catch {
                            case e: Exception => {
                                e.printStackTrace();
                                logger.error("error while translating json data: " + childValue + "\n \n \n" + e.getMessage());
                                System.exit(0)
                            }
                        }
                    } else {
                        logger.info("other format for child")
                    }

                    out = new PrintWriter(new BufferedWriter(new FileWriter(xR2RML_Constants.utilFilesave, false)))
                    out.println(parentValue)
                    out.flush()
                    out.close()

                    if (join.parentParse.getJoinParseFormat.equals(xR2RML_Constants.xR2RML_JSON_URI)) {
                        // in case the child is a json data
                        try {
                            var jsonR = Json.createReader(new FileReader(xR2RML_Constants.utilFilesave))
                            var jsonData = jsonR.read()
                            jsonR.close();

                            ParentList = jsonData.getValueType match {
                                case JsonValue.ValueType.OBJECT => {
                                    var obj = jsonData.asInstanceOf[JsonObject]
                                    var n = 0
                                    for (name <- obj.keySet) { n = n + 1 }
                                    var tab = Array.ofDim[String](n);
                                    n = 0
                                    for (name <- obj.keySet) { tab(n) = obj.get(name).toString(); n = n + 1 }
                                    tab
                                }
                                case JsonValue.ValueType.ARRAY => {
                                    var obj = jsonData.asInstanceOf[JsonArray]
                                    var n = 0
                                    for (name <- obj) { n = n + 1 }
                                    var tab = Array.ofDim[String](n);
                                    n = 0
                                    for (name <- obj) { tab(n) = name.toString(); n = n + 1 }
                                    tab
                                }
                                case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                                    logger.info("the data " + jsonData.toString() + "is not a list")
                                    System.exit(0)
                                    null
                                }
                                case _ => null
                            };
                        } catch {
                            case e: Exception => {
                                e.printStackTrace();
                                logger.error("error while translating json data: " + parentValue + "\n \n \n" + e.getMessage());
                                System.exit(0)
                            }
                        }
                    } else {
                        logger.info("other format for parent")
                    }

                    result = xR2RMLJsonUtils.compareList(ParentList, ChildList)

                } else if (childparsetype.equals(xR2RML_Constants.xR2RML_RRXLISTORMAP_URI) && parentparsetype.equals(Constants.R2RML_LITERAL_URI)) {

                    var ChildList = Array.ofDim[String](0);

                    var out = new PrintWriter(new BufferedWriter(new FileWriter(xR2RML_Constants.utilFilesave, false)))
                    out.println(childValue)
                    out.flush()
                    out.close()

                    if (join.childParse.getJoinParseFormat.equals(xR2RML_Constants.xR2RML_JSON_URI)) {
                        // in case the child is a json data
                        try {
                            var jsonR = Json.createReader(new FileReader(xR2RML_Constants.utilFilesave))
                            var jsonData = jsonR.read()
                            jsonR.close();

                            ChildList = jsonData.getValueType match {
                                case JsonValue.ValueType.OBJECT => {
                                    var obj = jsonData.asInstanceOf[JsonObject]
                                    var n = 0
                                    for (name <- obj.keySet) { n = n + 1 }
                                    var tab = Array.ofDim[String](n);
                                    n = 0
                                    for (name <- obj.keySet) { tab(n) = obj.get(name).toString(); n = n + 1 }
                                    tab
                                }
                                case JsonValue.ValueType.ARRAY => {
                                    var obj = jsonData.asInstanceOf[JsonArray]
                                    var n = 0
                                    for (name <- obj) { n = n + 1 }
                                    var tab = Array.ofDim[String](n);
                                    n = 0
                                    for (name <- obj) { tab(n) = name.toString(); n = n + 1 }
                                    tab
                                }
                                case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                                    logger.info("the data " + jsonData.toString() + "is not a list")
                                    System.exit(0)
                                    null
                                }
                                case _ => null
                            };

                        } catch {
                            case e: Exception => {
                                e.printStackTrace();
                                logger.error("error while translating json data: " + childValue + "\n \n \n" + e.getMessage());
                                System.exit(0)
                            }
                        }

                    } else {
                        logger.info("other format for child")
                    }
                    result = xR2RMLJsonUtils.containsString(ChildList, parentValue.toString())

                } else if (childparsetype.equals(Constants.R2RML_LITERAL_URI) && parentparsetype.equals(xR2RML_Constants.xR2RML_RRXLISTORMAP_URI)) {

                    var parentList = Array.ofDim[String](0);

                    var out = new PrintWriter(new BufferedWriter(new FileWriter(xR2RML_Constants.utilFilesave, false)))
                    out.println(parentValue)
                    out.flush()
                    out.close()

                    if (join.childParse.getJoinParseFormat.equals(xR2RML_Constants.xR2RML_JSON_URI)) {
                        // in case the child is a json data
                        try {
                            var jsonR = Json.createReader(new FileReader(xR2RML_Constants.utilFilesave))
                            var jsonData = jsonR.read()
                            jsonR.close();

                            parentList = jsonData.getValueType match {
                                case JsonValue.ValueType.OBJECT => {
                                    var obj = jsonData.asInstanceOf[JsonObject]
                                    var n = 0
                                    for (name <- obj.keySet) { n = n + 1 }
                                    var tab = Array.ofDim[String](n);
                                    n = 0
                                    for (name <- obj.keySet) { tab(n) = obj.get(name).toString(); n = n + 1 }
                                    tab
                                }
                                case JsonValue.ValueType.ARRAY => {
                                    var obj = jsonData.asInstanceOf[JsonArray]
                                    var n = 0
                                    for (name <- obj) { n = n + 1 }
                                    var tab = Array.ofDim[String](n);
                                    n = 0
                                    for (name <- obj) { tab(n) = name.toString(); n = n + 1 }
                                    tab
                                }
                                case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                                    logger.info("the data " + jsonData.toString() + "is not a list")
                                    System.exit(0)
                                    null
                                }
                                case _ => null
                            };

                        } catch {
                            case e: Exception => {
                                e.printStackTrace();
                                logger.error("error while translating json data: " + parentValue + "\n \n \n" + e.getMessage());
                                System.exit(0)
                            }
                        }

                    } else {
                        logger.info("other format for child")
                    }
                    result = xR2RMLJsonUtils.containsString(parentList, childValue.toString())

                } else {

                }

            }
        }

        return result;
    }

    // xR2RML

    def recursive_parser(properties: MorphProperties, materializer: MorphBaseMaterializer,
                         termMap: R2RMLTermMap, dbValue: Object, datatype: Option[String],
                         language: Option[String], rdfTriples: Boolean, defaultFormat: String): List[RDFNode] = {

        var result: List[RDFNode] = List(null);
        var format = termMap.getFomatType

        if (format.equals(xR2RML_Constants.xR2RML_RRXDEFAULTFORMAT_URI)) {
            format = defaultFormat
            logger.info("   Complete the code to get the source format " + format)
        }
        var out = new PrintWriter(new BufferedWriter(new FileWriter(xR2RML_Constants.utilFilesave, false)))
        out.println(dbValue.toString())
        out.flush()
        out.close()

        if (format.equals(xR2RML_Constants.xR2RML_JSON_URI)) {
            var jsonR = Json.createReader(new FileReader(xR2RML_Constants.utilFilesave))
            var jsonData = jsonR.read()
            jsonR.close();

            if (rdfTriples) {
                result = termMap match {
                    case _: R2RMLObjectMap => {
                        jsonData.getValueType match {

                            case JsonValue.ValueType.OBJECT => {
                                var resultList: List[RDFNode] = List()
                                var obj = jsonData.asInstanceOf[JsonObject]
                                var n = 0;
                                for (name <- obj.keySet) { n = n + 1; }
                                var tab = Array.ofDim[RDFNode](n);

                                n = 0;
                                if (termMap.hasRecursiveParse) {
                                    for (name <- obj.keySet) {
                                        tab(n) = navigateJSONTree(properties, materializer, termMap.getRecursiveParse.getRecursiveParse, Some(termMap.getRecursiveParse.getParseType.get), obj.get(name), datatype, language,
                                            Some(termMap.getRecursiveParse.getTermType));
                                        val l = List(tab(n))
                                        resultList = List.concat(l, resultList)
                                        n = n + 1;
                                    }
                                } else {
                                    for (name <- obj.keySet) {

                                        tab(n) = navigateJSONTree(properties, materializer, termMap.getRecursiveParse, Some(termMap.getParseType), obj.get(name), datatype, language, Some(termMap.inferTermType));
                                        val l = List(tab(n))
                                        resultList = List.concat(l, resultList)
                                        n = n + 1;
                                    }
                                }
                                resultList
                            }

                            case JsonValue.ValueType.ARRAY => {
                                var resultList: List[RDFNode] = List(null)
                                var obj = jsonData.asInstanceOf[JsonArray]
                                var n = 0;
                                for (name <- obj) { n = n + 1; }
                                var tab = Array.ofDim[RDFNode](n);
                                n = 0;
                                if (termMap.hasRecursiveParse) {
                                    for (name <- obj) {
                                        tab(n) = navigateJSONTree(properties, materializer, termMap.getRecursiveParse.getRecursiveParse, Some(termMap.getRecursiveParse.getParseType.get), name, datatype, language,
                                            Some(termMap.getRecursiveParse.getTermType));
                                        val l = List(tab(n))
                                        resultList = List.concat(l, resultList)
                                        n = n + 1;
                                    }
                                } else {
                                    for (name <- obj) {
                                        tab(n) = navigateJSONTree(properties, materializer, termMap.getRecursiveParse, Some(termMap.getParseType), name, datatype, language, Some(termMap.inferTermType));
                                        val l = List(tab(n))
                                        resultList = List.concat(l, resultList)
                                        n = n + 1;
                                    }
                                }
                                resultList
                            }
                            case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                                logger.info("the data " + jsonData.toString() + "is not a list")
                                System.exit(0)
                                null
                            }
                            case _ => null
                        }

                    }

                    case _: R2RMLPredicateMap => {
                        jsonData.getValueType match {

                            case JsonValue.ValueType.OBJECT => {
                                var resultList: List[RDFNode] = List()
                                var obj = jsonData.asInstanceOf[JsonObject]
                                var n = 0;
                                for (name <- obj.keySet) { n = n + 1; }
                                var tab = Array.ofDim[RDFNode](n);
                                n = 0;
                                for (name <- obj.keySet) {
                                    tab(n): RDFNode;
                                    tab(n) = translateJSONDataInRecursiveParseInLiteral(properties, materializer, Some(Constants.R2RML_IRI_URI), obj.get(name), datatype, language)
                                    val l = List(tab(n))

                                    resultList = List.concat(l, resultList)

                                    n = n + 1;
                                }
                                resultList
                            }

                            case JsonValue.ValueType.ARRAY => {
                                var resultList: List[RDFNode] = List(null)
                                var obj = jsonData.asInstanceOf[JsonArray]
                                var n = 0;
                                for (name <- obj) { n = n + 1; }
                                var tab = Array.ofDim[RDFNode](n);
                                n = 0;
                                for (name <- obj) {
                                    tab(n) = translateJSONDataInRecursiveParseInLiteral(properties, materializer, Some(Constants.R2RML_IRI_URI), name, datatype, language)
                                    val l = List(tab(n))

                                    resultList = List.concat(l, resultList)

                                    n = n + 1;
                                }
                                resultList
                            }
                            case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                                logger.info("the data " + jsonData.toString() + "is not a list")
                                System.exit(0)
                                null
                            }
                            case _ => null
                        }

                    }
                    case _: R2RMLSubjectMap => {
                        jsonData.getValueType match {

                            case JsonValue.ValueType.OBJECT => {
                                var resultList: List[RDFNode] = List()
                                var obj = jsonData.asInstanceOf[JsonObject]
                                var n = 0;
                                for (name <- obj.keySet) { n = n + 1; }
                                var tab = Array.ofDim[RDFNode](n);
                                n = 0;
                                for (name <- obj.keySet) {
                                    tab(n): RDFNode;
                                    tab(n) = translateJSONDataInRecursiveParseInLiteral(properties, materializer, Some(Constants.R2RML_IRI_URI), obj.get(name), datatype, language)
                                    val l = List(tab(n))

                                    resultList = List.concat(l, resultList)

                                    n = n + 1;
                                }
                                resultList
                            }

                            case JsonValue.ValueType.ARRAY => {
                                var resultList: List[RDFNode] = List(null)
                                var obj = jsonData.asInstanceOf[JsonArray]
                                var n = 0;
                                for (name <- obj) { n = n + 1; }
                                var tab = Array.ofDim[RDFNode](n);
                                n = 0;
                                for (name <- obj) {
                                    tab(n) = translateJSONDataInRecursiveParseInLiteral(properties, materializer, Some(Constants.R2RML_IRI_URI), name, datatype, language)
                                    val l = List(tab(n))

                                    resultList = List.concat(l, resultList)

                                    n = n + 1;
                                }
                                resultList
                            }
                            case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                                logger.info("the data " + jsonData.toString() + "is not a list")
                                System.exit(0)
                                null
                            }
                            case _ => null
                        }

                    }
                    case _ => { null }
                }
                result
            } else {

                try {
                    var jsonR = Json.createReader(new FileReader(xR2RML_Constants.utilFilesave))
                    var jsontree = jsonR.read()
                    jsonR.close();

                    var result = List(navigateJSONTree(properties, materializer, termMap.getRecursiveParse, Some(termMap.getParseType), jsontree, datatype, language, Some(termMap.inferTermType)))
                    result

                } catch {
                    case e: Exception => { logger.info(" error JSON format in database or parsing error"); System.exit(0); null }
                }
            }
            // other formats 
        } else if (format.equals(xR2RML_Constants.xR2RML_XML_URI)) {
            logger.info("XML Format")
            logger.info("Translating XML data into JSON for parsing")
            var data = xR2RML_XML_to_JSON.convertXMLinJSON(dbValue.toString())
            logger.info("Output JSON data")
            logger.info(data)
            termMap.setFomatType(xR2RML_Constants.xR2RML_JSON_URI)
            this.recursive_parser(properties, materializer, termMap, data, datatype, language, rdfTriples, defaultFormat)

        } else if (format.equals(xR2RML_Constants.xR2RML_RRXROW_URI)) {
            if (termMap.getRecursiveParse != null) { logger.info("Use of row Format with the parse property"); System.exit(0) }
            if (termMap.getParseType != null && termMap.getParseType == xR2RML_Constants.xR2RML_RRXLISTORMAP_URI) { logger.info("Use of parstype listOrMap with row Format"); System.exit(0) }

            List(translateDataRecursiveParseTypeLiteral(properties, materializer, Some(termMap.inferTermType), dbValue.toString(), datatype, language))

        } else if (format.equals(xR2RML_Constants.xR2RML_RRXCSV_URI)) {
            logger.info("CSV Format")
            logger.info("Translating CSV data into JSON for parsing")
            var data = xR2RML_CSV_to_JSON.convertCSVinJSON(dbValue.toString())
            logger.info("Output JSON data")
            logger.info(data)
            termMap.setFomatType(xR2RML_Constants.xR2RML_JSON_URI)
            this.recursive_parser(properties, materializer, termMap, data, datatype, language, rdfTriples, defaultFormat)

        } else {
            logger.info("   Unknow format " + format)
            null
        }
    }

    def getResultSetValueforJoinParse(rs: ResultSet, pColumnName: String, databaseType: String): Object = {
        try {
            val dbType = databaseType;
            val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);

            val zConstant = MorphSQLConstant(pColumnName, ZConstant.COLUMNNAME);
            val tableName = zConstant.table;

            val columnNameAux = zConstant.column

            val columnName = {
                if (tableName != null) {
                    tableName + "." + columnNameAux
                } else {
                    columnNameAux
                }
            }

            val result = rs.getString(columnName);

            result
        } catch {
            case e: Exception => {
                e.printStackTrace();
                logger.error("error occured when translating result: " + e.getMessage());
                null
            }
        }
    }

    def createIRI(properties: MorphProperties, originalIRI: String, materializer: MorphBaseMaterializer) = {
        var resultIRI = originalIRI;
        try {
            resultIRI = GeneralUtility.encodeURI(resultIRI, properties.mapURIEncodingChars, properties.uriTransformationOperation);
            if (properties != null) {
                if (properties.encodeUnsafeChars) {
                    resultIRI = GeneralUtility.encodeUnsafeChars(resultIRI);
                }

                if (properties.encodeReservedChars) {
                    resultIRI = GeneralUtility.encodeReservedChars(resultIRI);
                }
            }
            materializer.model.createResource(resultIRI);
        } catch {
            case e: Exception => {
                logger.warn("Error translating object uri value : " + resultIRI);
                throw e
            }
        }
    }

    def translateDateTime(value: String) = {
        value.toString().trim().replaceAll(" ", "T");
    }

    def translateBoolean(value: String) = {
        if (value.equalsIgnoreCase("T") || value.equalsIgnoreCase("True") || value.equalsIgnoreCase("1")) {
            "true";
        } else if (value.equalsIgnoreCase("F") || value.equalsIgnoreCase("False") || value.equalsIgnoreCase("0")) {
            "false";
        } else {
            "false";
        }
    }

    def createLiteral(value: Object, datatype: Option[String], language: Option[String], materializer: MorphBaseMaterializer): Literal = {

        val data: String = datatype.get
        try {
            val encodedValueAux = GeneralUtility.encodeLiteral(value.toString());

            val encodedValue = encodedValueAux;

            val valueWithDataType = if (data != null) {

                val xsdDateTimeURI = XSDDatatype.XSDdateTime.getURI().toString();
                val xsdBooleanURI = XSDDatatype.XSDboolean.getURI().toString();

                if (data.equals(xsdDateTimeURI)) {
                    this.translateDateTime(encodedValue);
                } else if (data.equals(xsdBooleanURI)) {
                    this.translateBoolean(encodedValue);
                } else {
                    encodedValue
                }

            } else {
                encodedValue
            }

            val result: Literal = if (language.isDefined) {
                materializer.model.createLiteral(valueWithDataType, language.get);
            } else {
                if (datatype.isDefined) {
                    materializer.model.createTypedLiteral(valueWithDataType, datatype.get);
                } else {
                    materializer.model.createLiteral(valueWithDataType);
                }
            }

            result
        } catch {
            case e: Exception => {
                logger.warn("Error translating object uri value : " + value);
                throw e
            }
        }
    }

    def translateJSONDataInRecursiveParseInLiteral(properties: MorphProperties, materializer: MorphBaseMaterializer, termT: Option[String],
                                                   jsonData: JsonValue, datatype: Option[String], languageTag: Option[String]): RDFNode = {
        var result = jsonData.getValueType match {
            case JsonValue.ValueType.OBJECT => {
                this.translateDataRecursiveParseTypeLiteral(properties, materializer, termT, jsonData.toString(), datatype, languageTag)
            }
            case JsonValue.ValueType.ARRAY => {
                this.translateDataRecursiveParseTypeLiteral(properties, materializer, termT, jsonData.toString(), datatype, languageTag)
            }
            case JsonValue.ValueType.STRING => {
                var st = jsonData.asInstanceOf[JsonString]
                this.translateDataRecursiveParseTypeLiteral(properties, materializer, termT, st.getString(), datatype, languageTag)
            }
            case JsonValue.ValueType.NUMBER => {
                var st = jsonData.asInstanceOf[JsonNumber]
                this.translateDataRecursiveParseTypeLiteral(properties, materializer, termT, st.toString(), datatype, languageTag)
            }
            case JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL => {
                this.translateDataRecursiveParseTypeLiteral(properties, materializer, termT, jsonData.toString(), datatype, languageTag)
            }
            case _ => null
        }
        result
    }

    def translateJSONDataInRecursiveParseForRdfTriples(properties: MorphProperties, materializer: MorphBaseMaterializer, termT: Option[String],
                                                       jsonData: JsonValue, datatype: Option[String], languageTag: Option[String]): RDFNode = {
        var result = jsonData.getValueType match {
            case JsonValue.ValueType.OBJECT => {
                this.translateDataRecursiveParseTypeLiteral(properties, materializer, termT, jsonData.toString(), datatype, languageTag)
            }
            case JsonValue.ValueType.ARRAY => {
                this.translateDataRecursiveParseTypeLiteral(properties, materializer, termT, jsonData.toString(), datatype, languageTag)
            }
            case JsonValue.ValueType.STRING => {
                var st = jsonData.asInstanceOf[JsonString]
                this.translateDataRecursiveParseTypeLiteral(properties, materializer, termT, st.getString(), datatype, languageTag)
            }
            case JsonValue.ValueType.NUMBER => {
                var st = jsonData.asInstanceOf[JsonNumber]
                this.translateDataRecursiveParseTypeLiteral(properties, materializer, termT, st.toString(), datatype, languageTag)
            }
            case JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL => {
                this.translateDataRecursiveParseTypeLiteral(properties, materializer, termT, jsonData.toString(), datatype, languageTag)
            }
            case _ => null
        }
        result
    }

    def translateDataRecursiveParseTypeLiteral(properties: MorphProperties, materializer: MorphBaseMaterializer, termtype: Option[String],
                                               dbValue: String, datatype: Option[String], languageTag: Option[String]): RDFNode = {
        var result = termtype.get match {
            case Constants.R2RML_IRI_URI => {
                if (dbValue != null) { this.createIRI(properties, dbValue.toString(), materializer); }
                else { null }
            }
            case Constants.R2RML_LITERAL_URI => {
                if (dbValue != null) {
                    this.createLiteral(dbValue, datatype, languageTag, materializer);
                } else { null }
            }
            case Constants.R2RML_BLANKNODE_URI => {
                if (dbValue != null) {
                    val anonId = new AnonId(dbValue.toString());
                    var rr = materializer.model.createResource(anonId)

                    rr
                } else { null }
            }
            // xR2RML
            case xR2RML_Constants.xR2RML_RDFLIST_URI => {
                if (dbValue != null) {
                    var tab = Array.ofDim[RDFNode](1);

                    tab(0) = this.createLiteral(dbValue, datatype, languageTag, materializer);

                    val list = ModelFactory.createDefaultModel().createList(tab)

                    list

                } else { null }
            }
            case xR2RML_Constants.xR2RML_RDFBAG_URI => {
                if (dbValue != null) {
                    var tab = Array.ofDim[RDFNode](1);
                    tab(0) = this.createLiteral(dbValue, datatype, languageTag, materializer);
                    val bag = ModelFactory.createDefaultModel().createBag()
                    bag.add(tab(0))

                    bag

                } else { null }
            }

            case xR2RML_Constants.xR2RML_RDFALT_URI => {
                if (dbValue != null) {
                    var tab = Array.ofDim[RDFNode](1);
                    tab(0) = this.createLiteral(dbValue, datatype, languageTag, materializer);
                    val alt = ModelFactory.createDefaultModel().createAlt()
                    alt.add(tab(0))

                    alt
                } else { null }
            }
            case xR2RML_Constants.xR2RML_RDFSEQ_URI => {
                if (dbValue != null) {
                    var tab = Array.ofDim[RDFNode](1);
                    tab(0) = this.createLiteral(dbValue, datatype, languageTag, materializer);
                    val seq = ModelFactory.createDefaultModel().createSeq()
                    seq.add(tab(0))
                    seq

                } else { null }
            }
            case _ => {
                null
            }
        }

        result
    }

    def navigateJSONTree(properties: MorphProperties, materializer: MorphBaseMaterializer, recPar: xR2RMLRecursiveParse, parsT: Option[String],
                         jsonData: JsonValue, datatypetag: Option[String], language: Option[String], termT: Option[String]): RDFNode = {
        // case there in not an embedded parse

        if (recPar == null) {

            var datatype = datatypetag
            var languageTag = language

            // if parsetype is not literal
            if (parsT.get == xR2RML_Constants.xR2RML_RRXLISTORMAP_URI) {

                if (termT.get == xR2RML_Constants.xR2RML_RDFTRIPLES_URI) {

                    var result = translateJSONDataInRecursiveParseInLiteral(properties, materializer, Some(Constants.R2RML_LITERAL_URI), jsonData,
                        datatype, languageTag)

                    result
                } else if (termT.get == xR2RML_Constants.xR2RML_RDFLIST_URI) {
                    var result =
                        jsonData.getValueType match {
                            case JsonValue.ValueType.OBJECT => {
                                var obj = jsonData.asInstanceOf[JsonObject]
                                var n = 0;
                                for (name <- obj.keySet) { n = n + 1; }
                                var tab = Array.ofDim[RDFNode](n);
                                n = 0;
                                for (name <- obj.keySet) {
                                    tab(n): RDFNode;
                                    tab(n) = this.translateDataRecursiveParseTypeLiteral(properties, materializer, Some(Constants.R2RML_LITERAL_URI), obj.get(name).toString(), datatype, languageTag);
                                    n = n + 1;
                                }
                                val list = ModelFactory.createDefaultModel().createList(tab)

                                list
                            }

                            case JsonValue.ValueType.ARRAY => {
                                var obj = jsonData.asInstanceOf[JsonArray]
                                var n = 0;
                                for (name <- obj) { n = n + 1; }
                                var tab = Array.ofDim[RDFNode](n);
                                n = 0;
                                for (name <- obj) {
                                    tab(n) = this.translateDataRecursiveParseTypeLiteral(properties, materializer, Some(Constants.R2RML_LITERAL_URI), name.toString(), datatype, languageTag);
                                    n = n + 1;
                                }
                                val list = ModelFactory.createDefaultModel().createList(tab)

                                list

                            }
                            case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                                logger.info("the data " + jsonData.toString() + "is not a list")
                                System.exit(0)
                                null
                            }
                            case _ => null
                        }
                    xR2RMLJsonUtils.saveNode(result)
                    result
                } else if (termT.get == xR2RML_Constants.xR2RML_RDFBAG_URI) {
                    var result =
                        jsonData.getValueType match {
                            case JsonValue.ValueType.OBJECT => {
                                var obj = jsonData.asInstanceOf[JsonObject]
                                var n = 0;
                                for (name <- obj.keySet) { n = n + 1; }
                                val bag = ModelFactory.createDefaultModel().createBag()
                                var tab = Array.ofDim[RDFNode](n);
                                n = 0;
                                for (name <- obj.keySet) {
                                    tab(n): RDFNode;
                                    tab(n) = translateDataRecursiveParseTypeLiteral(properties, materializer, Some(Constants.R2RML_LITERAL_URI), obj.get(name).toString(), datatype, languageTag);
                                    bag.add(tab(n))
                                    n = n + 1;
                                }

                                bag

                            }
                            case JsonValue.ValueType.ARRAY => {
                                var obj = jsonData.asInstanceOf[JsonArray]
                                var n = 0;
                                for (name <- obj) { n = n + 1; }
                                var tab = Array.ofDim[RDFNode](n);
                                val bag = ModelFactory.createDefaultModel().createBag()
                                n = 0;
                                for (name <- obj) {
                                    tab(n) = this.translateDataRecursiveParseTypeLiteral(properties, materializer, Some(Constants.R2RML_LITERAL_URI), name.toString(), datatype, languageTag);
                                    bag.add(tab(n))
                                    n = n + 1;
                                }

                                bag

                            }
                            case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                                logger.info("the data " + jsonData.toString() + "is not a list")
                                System.exit(0)
                                null
                            }
                            case _ => null
                        }
                    xR2RMLJsonUtils.saveNode(result)
                    result
                } else if (termT.get == xR2RML_Constants.xR2RML_RDFSEQ_URI) {
                    var result =
                        jsonData.getValueType match {
                            case JsonValue.ValueType.OBJECT => {
                                var obj = jsonData.asInstanceOf[JsonObject]
                                var n = 0;
                                for (name <- obj.keySet) { n = n + 1; }
                                val seq = ModelFactory.createDefaultModel().createSeq()
                                var tab = Array.ofDim[RDFNode](n);
                                n = 0;
                                for (name <- obj.keySet) {
                                    tab(n): RDFNode;
                                    tab(n) = this.translateDataRecursiveParseTypeLiteral(properties, materializer, Some(Constants.R2RML_LITERAL_URI), obj.get(name).toString(), datatype, languageTag);
                                    seq.add(tab(n))
                                    n = n + 1;
                                }

                                seq

                            }
                            case JsonValue.ValueType.ARRAY => {
                                var obj = jsonData.asInstanceOf[JsonArray]
                                var n = 0;
                                for (name <- obj) { n = n + 1; }
                                var tab = Array.ofDim[RDFNode](n);
                                val seq = ModelFactory.createDefaultModel().createSeq()
                                n = 0;
                                for (name <- obj) {
                                    tab(n) = this.translateDataRecursiveParseTypeLiteral(properties, materializer, Some(Constants.R2RML_LITERAL_URI), name.toString(), datatype, languageTag);
                                    seq.add(tab(n))
                                    n = n + 1;
                                }

                                seq

                            }
                            case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                                logger.info("the data " + jsonData.toString() + "is not a list")
                                System.exit(0)
                                null
                            }
                            case _ => null
                        }
                    xR2RMLJsonUtils.saveNode(result)
                    result

                } else if (termT.get == xR2RML_Constants.xR2RML_RDFALT_URI) {
                    var result =
                        jsonData.getValueType match {
                            case JsonValue.ValueType.OBJECT => {
                                var obj = jsonData.asInstanceOf[JsonObject]
                                var n = 0;
                                for (name <- obj.keySet) { n = n + 1; }
                                val alt = ModelFactory.createDefaultModel().createAlt()
                                var tab = Array.ofDim[RDFNode](n);
                                n = 0;
                                for (name <- obj.keySet) {
                                    tab(n): RDFNode;
                                    tab(n) = this.translateDataRecursiveParseTypeLiteral(properties, materializer, Some(Constants.R2RML_LITERAL_URI), obj.get(name).toString(), datatype, languageTag);
                                    alt.add(tab(n))
                                    n = n + 1;
                                }

                                alt

                            }
                            case JsonValue.ValueType.ARRAY => {
                                var obj = jsonData.asInstanceOf[JsonArray]
                                var n = 0;
                                for (name <- obj) { n = n + 1; }
                                var tab = Array.ofDim[RDFNode](n);
                                val alt = ModelFactory.createDefaultModel().createAlt()
                                n = 0;
                                for (name <- obj) {
                                    tab(n) = this.translateDataRecursiveParseTypeLiteral(properties, materializer, Some(Constants.R2RML_LITERAL_URI), name.toString(), datatype, languageTag);
                                    alt.add(tab(n))
                                    n = n + 1;
                                }

                                alt

                            }
                            case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                                logger.info("the data " + jsonData.toString() + "is not a list")
                                System.exit(0)
                                null
                            }
                            case _ => null
                        }
                    xR2RMLJsonUtils.saveNode(result)
                    result

                } else {
                    null
                }
            } else {

                // parseType == literal
                var result = this.translateJSONDataInRecursiveParseInLiteral(properties, materializer, termT, jsonData, datatype, languageTag);
                xR2RMLJsonUtils.saveNode(result)
                result
            }
        } // case there is an embedded parse
        else {
            var languageTag = Some(recPar.getlanguageTag.get)
            var datatype = Some(recPar.getdatatype.get)
            if (termT.get == xR2RML_Constants.xR2RML_RDFTRIPLES_URI) {
                println(" this case not done ")
                null
            } else if (termT.get == xR2RML_Constants.xR2RML_RDFLIST_URI) {
                var result =
                    jsonData.getValueType match {
                        case JsonValue.ValueType.OBJECT => {
                            var obj = jsonData.asInstanceOf[JsonObject]
                            var n = 0;
                            for (name <- obj.keySet) { n = n + 1; }
                            var tab = Array.ofDim[RDFNode](n);
                            n = 0;
                            for (name <- obj.keySet) {
                                tab(n): RDFNode;
                                tab(n) = this.navigateJSONTree(properties, materializer, recPar.getRecursiveParse, recPar.getParseType, obj.get(name), datatype, languageTag, Some(recPar.getTermType));
                                n = n + 1;
                            }
                            val list = ModelFactory.createDefaultModel().createList(tab)
                            list
                        }
                        case JsonValue.ValueType.ARRAY => {
                            var obj = jsonData.asInstanceOf[JsonArray]
                            var n = 0;
                            for (name <- obj) { n = n + 1; }
                            var tab = Array.ofDim[RDFNode](n);
                            n = 0;
                            for (name <- obj) {
                                tab(n) = this.navigateJSONTree(properties, materializer, recPar.getRecursiveParse, recPar.getParseType, name, datatype, languageTag, Some(recPar.getTermType));
                                n = n + 1;
                            }
                            val list = ModelFactory.createDefaultModel().createList(tab)
                            list
                        }
                        case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                            logger.info("the data " + jsonData.toString() + "is not a list")
                            System.exit(0)
                            null
                        }
                        case _ => null
                    }
                xR2RMLJsonUtils.saveNode(result)
                result
            } else if (termT.get == xR2RML_Constants.xR2RML_RDFBAG_URI) {
                var result =
                    jsonData.getValueType match {
                        case JsonValue.ValueType.OBJECT => {
                            var obj = jsonData.asInstanceOf[JsonObject]
                            var n = 0;
                            for (name <- obj.keySet) { n = n + 1; }
                            val bag = ModelFactory.createDefaultModel().createBag()
                            var tab = Array.ofDim[RDFNode](n);
                            n = 0;
                            for (name <- obj.keySet) {
                                tab(n): RDFNode;
                                tab(n) = this.navigateJSONTree(properties, materializer, recPar.getRecursiveParse, recPar.getParseType, obj.get(name), datatype, languageTag, Some(recPar.getTermType));
                                bag.add(tab(n))
                                n = n + 1;
                            }
                            bag

                        }
                        case JsonValue.ValueType.ARRAY => {
                            var obj = jsonData.asInstanceOf[JsonArray]
                            var n = 0;
                            for (name <- obj) { n = n + 1; }
                            var tab = Array.ofDim[RDFNode](n);
                            val bag = ModelFactory.createDefaultModel().createBag()
                            n = 0;
                            for (name <- obj) {
                                tab(n) = this.navigateJSONTree(properties, materializer, recPar.getRecursiveParse, recPar.getParseType, name, datatype, languageTag, Some(recPar.getTermType));
                                bag.add(tab(n))
                                n = n + 1;
                            }
                            bag

                        }
                        case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                            logger.info("the data " + jsonData.toString() + "is not a list")
                            System.exit(0)
                            null
                        }
                        case _ => null
                    }
                xR2RMLJsonUtils.saveNode(result)
                result
            } else if (termT.get == xR2RML_Constants.xR2RML_RDFSEQ_URI) {
                var result =
                    jsonData.getValueType match {
                        case JsonValue.ValueType.OBJECT => {
                            var obj = jsonData.asInstanceOf[JsonObject]
                            var n = 0;
                            for (name <- obj.keySet) { n = n + 1; }
                            val seq = ModelFactory.createDefaultModel().createSeq()
                            var tab = Array.ofDim[RDFNode](n);
                            n = 0;
                            for (name <- obj.keySet) {
                                tab(n): RDFNode;
                                tab(n) = this.navigateJSONTree(properties, materializer, recPar.getRecursiveParse, recPar.getParseType, obj.get(name), datatype, languageTag, Some(recPar.getTermType));
                                seq.add(tab(n))
                                n = n + 1;
                            }
                            seq

                        }
                        case JsonValue.ValueType.ARRAY => {
                            var obj = jsonData.asInstanceOf[JsonArray]
                            var n = 0;
                            for (name <- obj) { n = n + 1; }
                            var tab = Array.ofDim[RDFNode](n);
                            val seq = ModelFactory.createDefaultModel().createSeq()
                            n = 0;
                            for (name <- obj) {
                                tab(n) = this.navigateJSONTree(properties, materializer, recPar.getRecursiveParse, recPar.getParseType, name, datatype, languageTag, Some(recPar.getTermType));
                                seq.add(tab(n))
                                n = n + 1;
                            }
                            seq

                        }
                        case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                            logger.info("the data " + jsonData.toString() + "is not a list")
                            System.exit(0)
                            null
                        }
                        case _ => null
                    }
                xR2RMLJsonUtils.saveNode(result)
                result

            } else if (termT.get == xR2RML_Constants.xR2RML_RDFALT_URI) {
                var result =
                    jsonData.getValueType match {
                        case JsonValue.ValueType.OBJECT => {
                            var obj = jsonData.asInstanceOf[JsonObject]
                            var n = 0;
                            for (name <- obj.keySet) { n = n + 1; }
                            val alt = ModelFactory.createDefaultModel().createAlt()
                            var tab = Array.ofDim[RDFNode](n);
                            n = 0;
                            for (name <- obj.keySet) {
                                tab(n): RDFNode;
                                tab(n) = this.navigateJSONTree(properties, materializer, recPar.getRecursiveParse, recPar.getParseType, obj.get(name), datatype, languageTag, Some(recPar.getTermType));
                                alt.add(tab(n))
                                n = n + 1;
                            }
                            alt

                        }
                        case JsonValue.ValueType.ARRAY => {
                            var obj = jsonData.asInstanceOf[JsonArray]
                            var n = 0;
                            for (name <- obj) { n = n + 1; }
                            var tab = Array.ofDim[RDFNode](n);
                            val alt = ModelFactory.createDefaultModel().createAlt()
                            n = 0;
                            for (name <- obj) {
                                tab(n) = this.navigateJSONTree(properties, materializer, recPar.getRecursiveParse, recPar.getParseType, name, datatype, languageTag, Some(recPar.getTermType));
                                alt.add(tab(n))
                                n = n + 1;
                            }
                            alt

                        }
                        case JsonValue.ValueType.NUMBER | JsonValue.ValueType.TRUE | JsonValue.ValueType.FALSE | JsonValue.ValueType.NULL | JsonValue.ValueType.STRING => {
                            logger.info("the data " + jsonData.toString() + "is not a list")
                            System.exit(0)
                            null
                        }
                        case _ => null
                    }
                xR2RMLJsonUtils.saveNode(result)
                result

            } else {
                null
            }

        }
    }

}
