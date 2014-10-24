package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.rdf.model.RDFNode
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class R2RMLObjectMap(termMapType: Constants.MorphTermMapType.Value, termType: Option[String], datatype: Option[String], languageTag: Option[String], format: Option[String], parseType: Option[String], recursive_parse: xR2RMLRecursiveParse)
  extends R2RMLTermMap(termMapType, termType, datatype, languageTag, format, parseType, recursive_parse) {

  override val logger = Logger.getLogger(this.getClass().getName());

  var parset = this.getParseType
  var termtype = this.inferTermType
  val pars = this.getRecursiveParse

  if (parset != null && parset.equals(xR2RML_Constants.xR2RML_RRXLISTORMAP_URI)) {
    if (termtype != xR2RML_Constants.xR2RML_RDFALT_URI && termtype != xR2RML_Constants.xR2RML_RDFSEQ_URI
      && termtype != xR2RML_Constants.xR2RML_RDFBAG_URI && termtype != xR2RML_Constants.xR2RML_RDFLIST_URI
      && termtype != xR2RML_Constants.xR2RML_RDFTRIPLES_URI) {
      throw new Exception("Illegal termtype in termMap " + termMapType);
    }
  }

  if (parset != null) {
    if (!parset.equals(Constants.R2RML_LITERAL_URI) && !parset.equals(xR2RML_Constants.xR2RML_RRXLISTORMAP_URI)) {
      throw new Exception("Illegal parsetype value in termMap " + termMapType);
    }
  }

  if (parset != null && parset.equals(Constants.R2RML_LITERAL_URI) && pars != null) {
    throw new Exception("Illegal termMap description " + termMapType);
  }
}

object R2RMLObjectMap {
  def apply(rdfNode: RDFNode, formatFromLogicalTable: String): R2RMLObjectMap = {
    val coreProperties = R2RMLTermMap.extractCoreProperties(rdfNode);
    //coreProperties = (termMapType, termType, datatype, languageTag)
    val termMapType = coreProperties._1;
    val termType = coreProperties._2;
    val datatype = coreProperties._3;
    val languageTag = coreProperties._4;
    var format = coreProperties._5;
    val parseType = coreProperties._6;
    val recursive_parse = coreProperties._7;

    if (format == null || !format.isDefined) {
      format = Some(formatFromLogicalTable)
    }
    val om = new R2RMLObjectMap(termMapType, termType, datatype, languageTag, format, parseType, recursive_parse);
    om.parse(rdfNode);
    om;
  }

  def extractObjectMaps(resource: Resource, formatFromLogicalTable: String): Set[R2RMLObjectMap] = {
    val tms = R2RMLTermMap.extractTermMaps(resource, Constants.MorphPOS.obj, formatFromLogicalTable);
    val result = tms.map(tm => tm.asInstanceOf[R2RMLObjectMap]);
    result;
  }
}