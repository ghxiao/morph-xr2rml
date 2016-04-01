package es.upm.fi.dia.oeg.morph.base.materializer

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.RDFNode
import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.rdf.model.ModelFactory
import java.io.File
import com.hp.hpl.jena.tdb.TDBFactory
import java.io.OutputStream
import java.io.Writer

abstract class MorphBaseMaterializer(val model: Model, var outputStream: Writer) {
    val logger = Logger.getLogger(this.getClass.getName);
    var rdfLanguage: String = null;

    /**
     * Serialize the current model to the output file
     */
    def materialize

    /**
     * Utility method to serialize any model into the output file
     */
    def materialize(model: Model)

    /**
     * Materialize one triple in a target graph
     */
    def materializeQuad(subject: RDFNode, predicate: RDFNode, obj: RDFNode, graph: RDFNode)

    /**
     * Materialize multiple triples in a target graph
     *
     * @return number of triples generated
     */
    def materializeQuads(
        subjects: List[RDFNode],
        predicates: List[RDFNode],
        objects: List[RDFNode],
        refObjects: List[RDFNode],
        graphs: List[RDFNode]): Integer

    def setModelPrefixMap(prefixMap: Map[String, String]) = {
        this.model.setNsPrefixes(prefixMap);
    }
}

object MorphBaseMaterializer {
    val logger = Logger.getLogger(this.getClass.getName);

    def createJenaModel(jenaMode: String): Model = {
        val model =
            if (jenaMode == null)
                MorphBaseMaterializer.createJenaMemoryModel;
            else {
                if (jenaMode.equalsIgnoreCase(Constants.JENA_MODE_TYPE_TDB))
                    MorphBaseMaterializer.createJenaTDBModel
                else if (jenaMode.equalsIgnoreCase(Constants.JENA_MODE_TYPE_MEMORY))
                    MorphBaseMaterializer.createJenaMemoryModel
                else
                    MorphBaseMaterializer.createJenaMemoryModel
            }
        model
    }

    def createJenaMemoryModel: Model = { ModelFactory.createDefaultModel; }

    def createJenaTDBModel: Model = {
        val jenaDatabaseName = System.currentTimeMillis + "";
        val tdbDatabaseFolder = "tdb-database";
        val folder = new File(tdbDatabaseFolder);
        if (!folder.exists) 
            folder.mkdir
            
        val tdbFileBase = tdbDatabaseFolder + "/" + jenaDatabaseName;
        logger.info("TDB filebase = " + tdbFileBase);
        return TDBFactory.createModel(tdbFileBase);
    }

}