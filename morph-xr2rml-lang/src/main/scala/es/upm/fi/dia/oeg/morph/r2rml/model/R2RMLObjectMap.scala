package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.rdf.model.RDFNode
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class R2RMLObjectMap(
    termMapType: Constants.MorphTermMapType.Value,
    termType: Option[String],
    datatype: Option[String],
    languageTag: Option[String],
    nestedTermMap: Option[xR2RMLNestedTermMap],
    /** Reference formulation from the logical source */
    refFormulation: String)

        extends R2RMLTermMap(termMapType, termType, datatype, languageTag, nestedTermMap, refFormulation) {

    override val logger = Logger.getLogger(this.getClass().getName());
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
        val nestedTermMap = coreProperties._5;

        val om = new R2RMLObjectMap(termMapType, termType, datatype, languageTag, nestedTermMap, refFormulation);
        om.parse(rdfNode);
        om;
    }

    def extractObjectMaps(resource: Resource, formatFromLogicalTable: String): Set[R2RMLObjectMap] = {
        logger.trace("Looking for object maps")
        val tms = R2RMLTermMap.extractTermMaps(resource, Constants.MorphPOS.obj, formatFromLogicalTable);
        val result = tms.map(tm => tm.asInstanceOf[R2RMLObjectMap]);
        if (result.isEmpty)
            logger.trace("No graph map found.")
        result;
    }
}