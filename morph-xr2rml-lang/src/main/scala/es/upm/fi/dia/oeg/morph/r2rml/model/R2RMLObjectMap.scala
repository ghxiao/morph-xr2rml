package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._

import org.apache.log4j.Logger

import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.Resource

import es.upm.fi.dia.oeg.morph.base.Constants

class R2RMLObjectMap(
    termMapType: Constants.MorphTermMapType.Value,
    termType: Option[String],
    datatype: Option[String],
    languageTag: Option[String],
    nestedTermMap: Option[xR2RMLNestedTermMap],
    refFormulation: String)

        extends R2RMLTermMap(termMapType, termType, datatype, languageTag, nestedTermMap, refFormulation) {

    var termtype = this.inferTermType
}

object R2RMLObjectMap {
    val logger = Logger.getLogger(this.getClass().getName());

    def apply(rdfNode: RDFNode, refFormulation: String): R2RMLObjectMap = {
        val coreProperties = R2RMLTermMap.extractCoreProperties(rdfNode);
        val termMapType = coreProperties._1;
        val termType = coreProperties._2;
        val datatype = coreProperties._3;
        val languageTag = coreProperties._4;
        var nestedTermMap = coreProperties._5;

        // A term map with an RDF collection/container term type must have a nested term map.
        // If this is not the case here, define a default nested term type (see xR2RML specification 3.2.1.3):
        // it has term type rr:Literal if the parent term map is column- or reference-valued,
        // it has term type rr:iri if the parent term map is template-valued.
        if (R2RMLTermMap.isRdfCollectionTermType(termType) && (!nestedTermMap.isDefined)) {
            val ntmTermType = termMapType match {
                case Constants.MorphTermMapType.ColumnTermMap => Constants.R2RML_LITERAL_URI
                case Constants.MorphTermMapType.ReferenceTermMap => Constants.R2RML_LITERAL_URI
                case Constants.MorphTermMapType.TemplateTermMap => Constants.R2RML_IRI_URI
                case _ => Constants.R2RML_LITERAL_URI
            }
            nestedTermMap = Some(new xR2RMLNestedTermMap(termMapType, Some(ntmTermType), None, None, None))
        }

        val om = new R2RMLObjectMap(termMapType, termType, datatype, languageTag, nestedTermMap, refFormulation);
        om.parse(rdfNode);
        om;
    }

    /**
     * Create a set of ObjectMaps by checking the rr:objectMap properties of a PredicateObjectMap
     *
     * @param resource A Jena node representing a PredicateObjectMap instance
     * @param refFormulation the current reference formulation given in the configuration file
     * @return a possibly empty set of R2RMLRefObjectMap's
     */
    def extractObjectMaps(resource: Resource, refFormulation: String): Set[R2RMLObjectMap] = {
        logger.trace("Looking for object maps")
        val tms = R2RMLTermMap.extractTermMaps(resource, Constants.MorphPOS.obj, refFormulation);
        val result = tms.map(tm => tm.asInstanceOf[R2RMLObjectMap]);
        if (result.isEmpty)
            logger.trace("No object map found.")
        result;
    }
}