package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.RDFNode
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class R2RMLPredicateMap(termMapType: Constants.MorphTermMapType.Value, termType: Option[String], datatype: Option[String], languageTag: Option[String], format: Option[String], parseType: Option[String], recursive_parse: xR2RMLRecursiveParse)
  extends R2RMLTermMap(termMapType, termType, datatype, languageTag, format, parseType, recursive_parse) {

  var parset = this.getParseType
  var termtype = this.inferTermType

  if (termtype != null && !termtype.equals(Constants.R2RML_IRI_URI) && !termtype.equals(xR2RML_Constants.xR2RML_RDFTRIPLES_URI)) {
    throw new Exception("Illegal termtype in predicateMap ");
  }

  if (parset != null) {
    if (!parset.equals(Constants.R2RML_LITERAL_URI) && !parset.equals(xR2RML_Constants.xR2RML_RRXLISTORMAP_URI)) {
      throw new Exception("Illegal parsetype value in predicateMap ");
    }
  }

  val pars = this.getRecursiveParse
  if (pars != null) { throw new Exception("Illegal parse in predicateMap "); }
}

object R2RMLPredicateMap {
  def apply(rdfNode: RDFNode, formatFromLogicalTable: String): R2RMLPredicateMap = {
    val coreProperties = R2RMLTermMap.extractCoreProperties(rdfNode);
    //coreProperties = (termMapType, termType, datatype, languageTag)
    val termMapType = coreProperties._1;
    val termType = coreProperties._2;
    val datatype = coreProperties._3;
    val languageTag = coreProperties._4;
    var format = coreProperties._5;
    val parseType = coreProperties._6;
    val recursive_parse = coreProperties._7;
    if (!format.isDefined || format == null) {
      format = Some(formatFromLogicalTable)
    }
    val pm = new R2RMLPredicateMap(termMapType, termType, datatype, languageTag, format, parseType, recursive_parse);
    pm.parse(rdfNode);
    pm;
  }

  def extractPredicateMaps(resource: Resource, formatFromLogicalTable: String): Set[R2RMLPredicateMap] = {
    val tms = R2RMLTermMap.extractTermMaps(resource, Constants.MorphPOS.pre, formatFromLogicalTable);
    val result = tms.map(tm => tm.asInstanceOf[R2RMLPredicateMap]);
    result;
  }
}