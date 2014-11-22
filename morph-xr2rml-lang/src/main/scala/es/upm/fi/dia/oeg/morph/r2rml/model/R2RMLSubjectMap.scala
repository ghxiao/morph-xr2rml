package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.rdf.model.Resource
import java.util.HashSet
import com.hp.hpl.jena.rdf.model.RDFNode
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class R2RMLSubjectMap(
    termMapType: Constants.MorphTermMapType.Value,
    termType: Option[String],
    datatype: Option[String],
    languageTag: Option[String],

    /** IRIs of classes defined with the rr:class property */
    val classURIs: Set[String],

    /** IRIs of target graphs defined in the subject map with the rr:graph or rr:graphMap properties */
    val graphMaps: Set[R2RMLGraphMap],

    /** Reference formulation from the logical source */
    refFormulaion: String)

        extends R2RMLTermMap(termMapType, termType, datatype, languageTag, None, refFormulaion) {

    override val logger = Logger.getLogger(this.getClass().getName());
    var termtype = this.inferTermType
}

object R2RMLSubjectMap {
    val logger = Logger.getLogger(this.getClass().getName());

    def apply(rdfNode: RDFNode, refFormulation: String): R2RMLSubjectMap = {
        val coreProperties = R2RMLTermMap.extractCoreProperties(rdfNode);
        val termMapType = coreProperties._1;
        val termType = coreProperties._2;
        val datatype = coreProperties._3;
        val languageTag = coreProperties._4;

        /**
         * @TODO in xR2RML: should we raise an error in case of a nested term map?
         * It is allowed by the spec but is it really clever?
         */

        // List the optional rr:class properties of the subject map
        val classURIs: Set[String] = rdfNode match {
            case resourceNode: Resource => {
                val classStatements = resourceNode.listProperties(Constants.R2RML_CLASS_PROPERTY);
                val classURIsAux: Set[String] =
                    if (classStatements != null) {
                        classStatements.map(classStatement => { classStatement.getObject().toString(); }).toSet;
                    } else {
                        Set.empty;
                    }
                logger.trace("Found rr:class: " + classURIsAux)
                classURIsAux
            }
            case _ => { Set.empty }
        }

        // Find the optional rr:graph of rr:graphMap properties of the subject map
        val graphMaps: Set[R2RMLGraphMap] = rdfNode match {
            case resourceNode: Resource => { R2RMLGraphMap.extractGraphMaps(resourceNode, refFormulation); }
            case _ => { Set.empty }
        }

        val sm = new R2RMLSubjectMap(termMapType, termType, datatype, languageTag, classURIs, graphMaps, refFormulation);
        sm.rdfNode = rdfNode;

        sm.parse(rdfNode)
        sm
    }

    def extractSubjectMaps(resource: Resource, refFormulation: String): Set[R2RMLSubjectMap] = {
        logger.trace("Looking for subject maps")
        val tms = R2RMLTermMap.extractTermMaps(resource, Constants.MorphPOS.sub, refFormulation);
        val result = tms.map(tm => tm.asInstanceOf[R2RMLSubjectMap]);
        result;
    }
}