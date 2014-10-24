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
    termType: Option[String], datatype: Option[String],
    languageTag: Option[String],

    /** IRIs of classes defined with the rr:class property */
    val classURIs: Set[String],

    /** IRIs of target graphs defined in the subject map with the rr:graph or rr:graphMap properties */
    val graphMaps: Set[R2RMLGraphMap],

    format: Option[String],
    parseType: Option[String],
    recursive_parse: xR2RMLRecursiveParse)
        extends R2RMLTermMap(termMapType, termType, datatype, languageTag, format, parseType, recursive_parse) {

    override val logger = Logger.getLogger(this.getClass().getName());

    var parset = this.getParseType
    var termtype = this.inferTermType

    if (termtype != null && !termtype.equals(Constants.R2RML_IRI_URI) && !termtype.equals(Constants.R2RML_BLANKNODE_URI)
        && !termtype.equals(xR2RML_Constants.xR2RML_RDFTRIPLES_URI)) {
        throw new Exception("Illegal termtype in subjectMap ");
    }

    if (parset != null) {
        if (!parset.equals(Constants.R2RML_LITERAL_URI) && !parset.equals(xR2RML_Constants.xR2RML_RRXLISTORMAP_URI)) {
            throw new Exception("Illegal parsetype value in subjectMap ");
        }
    }

    val pars = this.getRecursiveParse
    if (pars != null) { throw new Exception("Illegal parse in subjectMap "); }
}

object R2RMLSubjectMap {
    val logger = Logger.getLogger(this.getClass().getName());

    def apply(rdfNode: RDFNode, formatFromLogicalTable: String): R2RMLSubjectMap = {
        val coreProperties = R2RMLTermMap.extractCoreProperties(rdfNode);

        val termMapType = coreProperties._1;
        val termType = coreProperties._2;
        val datatype = coreProperties._3;
        val languageTag = coreProperties._4;
        var format = coreProperties._5;
        val parseType = coreProperties._6;
        val recursive_parse = coreProperties._7;

        val classURIs: Set[String] = rdfNode match {
            case resourceNode: Resource => {
                val classStatements = resourceNode.listProperties(Constants.R2RML_CLASS_PROPERTY);
                val classURIsAux: Set[String] = if (classStatements != null) {
                    classStatements.map(classStatement => {
                        classStatement.getObject().toString();
                    }).toSet;
                } else {
                    Set.empty;
                }
                classURIsAux
            }
            case _ => { Set.empty }
        }

        val graphMaps: Set[R2RMLGraphMap] = rdfNode match {
            case resourceNode: Resource => { R2RMLGraphMap.extractGraphMaps(resourceNode, formatFromLogicalTable); }
            case _ => { Set.empty }
        }

        if (format == null || !format.isDefined) {
            format = Some(formatFromLogicalTable)
        }

        val sm = new R2RMLSubjectMap(termMapType, termType, datatype, languageTag, classURIs, graphMaps, format, parseType, recursive_parse);
        sm.rdfNode = rdfNode;

        sm.parse(rdfNode)
        sm
    }

    def extractSubjectMaps(resource: Resource, format: String): Set[R2RMLSubjectMap] = {
        val tms = R2RMLTermMap.extractTermMaps(resource, Constants.MorphPOS.sub, format);
        val result = tms.map(tm => tm.asInstanceOf[R2RMLSubjectMap]);
        result;
    }

}