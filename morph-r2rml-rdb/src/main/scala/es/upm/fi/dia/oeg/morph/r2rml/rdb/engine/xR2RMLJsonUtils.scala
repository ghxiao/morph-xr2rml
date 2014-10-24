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

object xR2RMLJsonUtils {
    val logger = Logger.getLogger(this.getClass().getName());

    // check if the array tab contains the string data
    def containsString(tab: Array[String], data: String): Boolean = {
        var bool = false
        for (n <- tab) { if (n.equals(data)) { bool = true } }
        return bool
    }

    // check if one of the array is included in the second
    def compareList(l1: Array[String], l2: Array[String]): Boolean = {
        var bool1 = true
        var bool2 = true
        for (n <- l1) { if (!this.containsString(l2, n)) { bool1 = false } }
        for (n <- l2) { if (!this.containsString(l1, n)) { bool2 = false } }

        if (bool1 || bool2) {
            return true
        } else {
            return false
        }
    }

    // save the content of the node
    def saveNode(node: RDFNode) {
        if (node != null) {
            // Write the model to a file
            node.getModel().write(new PrintStream(new File(xR2RML_Constants.utilFilesaveTrash)), "N-TRIPLE", null)
            
            // Read the file to a buffered reader and copy lines to a string "data"
            var data = "";
            val br = new BufferedReader(new FileReader(xR2RML_Constants.utilFilesaveTrash));
            var bool = true
            while (bool) {
                val line = br.readLine()
                if (line == null) { bool = false; }
                else { data = data + line + ""; }
            }
            br.close()

            // And then write it again to another file.... ?!!??#?@?!?!?
            try {
                var out = new PrintWriter(new BufferedWriter(new FileWriter(xR2RML_Constants.utilFilesavegraph, true)))
                out.println(data + "\n")
                out.flush()
                out.close()
            } catch {
                case e: IOException => {}
            }

        }
    }
}