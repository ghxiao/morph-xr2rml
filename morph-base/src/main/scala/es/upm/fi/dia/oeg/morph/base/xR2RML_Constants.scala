package es.upm.fi.dia.oeg.morph.base

import com.hp.hpl.jena.rdf.model.ResourceFactory

class xR2RML_Constants {

}

// Contains the constants for xR2RML
object xR2RML_Constants {

  val utilFilesavegraph = "savegraph.xrr"
  val utilFilesave = "save.xrr"
  val utilFilesaveTrash = "savegraphTrash.xrr"

  val xR2RML_NS = "http://i3s.unice.fr/xr2rml#";
  val RML_NS = "http://mmlab.be/rml#";
  val QL_NS = "http://semweb.mmlab.be/ns/ql#";

  //TriplesMap
  val xR2RML_LOGICALSOURCE_URI = xR2RML_NS + "logicalSource";
  val xR2RML_LOGICALSOURCE_PROPERTY = ResourceFactory.createProperty(xR2RML_LOGICALSOURCE_URI);

  //logical source
  val xR2RML_SOURCENAME_URI = xR2RML_NS + "sourceName";
  val xR2RML_SOURCENAME_PROPERTY = ResourceFactory.createProperty(xR2RML_SOURCENAME_URI);
  val xR2RML_RRXQUERY_URI = xR2RML_NS + "query";
  val xR2RML_RRXQUERY_PROPERTY = ResourceFactory.createProperty(xR2RML_RRXQUERY_URI);

  // format
  val xR2RML_FORMAT_URI = xR2RML_NS + "format";
  val xR2RML_FORMAT_PROPERTY = ResourceFactory.createProperty(xR2RML_FORMAT_URI);

  val xR2RML_FORMATSEQ_URI = xR2RML_NS + "formatSeq";
  val xR2RML_FORMATSEQ_PROPERTY = ResourceFactory.createProperty(xR2RML_FORMATSEQ_URI);

  val xR2RML_JSON_URI = xR2RML_NS + "JSON";
  val xR2RML_JSON_CLASS = ResourceFactory.createResource(xR2RML_JSON_URI);

  val xR2RML_XML_URI = xR2RML_NS + "XML";
  val xR2RML_XML_CLASS = ResourceFactory.createResource(xR2RML_XML_URI);

  val xR2RML_RRXROW_URI = xR2RML_NS + "Row";
  val xR2RML_RRXROW_CLASS = ResourceFactory.createResource(xR2RML_RRXROW_URI);

  val xR2RML_RRXCSV_URI = xR2RML_NS + "CSV";
  val xR2RML_RRXCSV_CLASS = ResourceFactory.createResource(xR2RML_RRXCSV_URI);

  val xR2RML_RRXDEFAULTFORMAT_URI = xR2RML_NS + "defaultFormat";
  val xR2RML_RRXDEFAULTFORMAT_CLASS = ResourceFactory.createResource(xR2RML_RRXDEFAULTFORMAT_URI);

  //TermMap Types
  val xR2RML_REFERENCE_URI = xR2RML_NS + "reference";
  val xR2RML_REFERENCE_PROPERTY = ResourceFactory.createProperty(xR2RML_REFERENCE_URI);

  //TermType
  val xR2RML_TERMTYPE_URI = xR2RML_NS + "termType";
  val xR2RML_TERMTYPE_PROPERTY = ResourceFactory.createProperty(xR2RML_TERMTYPE_URI);

  val xR2RML_RDFLIST_URI = xR2RML_NS + "RdfList";
  val xR2RML_RDFLIST_CLASS = ResourceFactory.createResource(xR2RML_RDFLIST_URI);

  val xR2RML_RDFBAG_URI = xR2RML_NS + "RdfBag";
  val xR2RML_RDFBAG_CLASS = ResourceFactory.createResource(xR2RML_RDFBAG_URI);

  val xR2RML_RDFSEQ_URI = xR2RML_NS + "RdfSeq";
  val xR2RML_RDFSEQ_CLASS = ResourceFactory.createResource(xR2RML_RDFSEQ_URI);

  val xR2RML_RDFALT_URI = xR2RML_NS + "RdfAlt";
  val xR2RML_RDFALT_CLASS = ResourceFactory.createResource(xR2RML_RDFALT_URI);

  val xR2RML_RDFTRIPLES_URI = xR2RML_NS + "rdfTriples";
  val xR2RML_RDFTRIPLES_CLASS = ResourceFactory.createResource(xR2RML_RDFTRIPLES_URI);

  val xR2RML_TEMPLATE_PATTERN = "\\{\"*\\w+(\\s\\w+)*\\\"*}";

  // xR2RML Properties
  val xR2RML_OBJECTMAP_URI = xR2RML_NS + "objectMap";
  val xR2RML_OBJECTMAP_PROPERTY = ResourceFactory.createProperty(xR2RML_OBJECTMAP_URI);

  // PArse Types
  val xR2RML_PARSETYPE_URI = xR2RML_NS + "parseType";
  val xR2RML_PARSETYPE_PROPERTY = ResourceFactory.createProperty(xR2RML_PARSETYPE_URI);

  val xR2RML_PARSETYPESEQ_URI = xR2RML_NS + "parseTypeSeq";
  val xR2RML_PARSETYPESEQ_PROPERTY = ResourceFactory.createProperty(xR2RML_PARSETYPESEQ_URI);

  val xR2RML_RRXLISTORMAP_URI = xR2RML_NS + "listOrMap";
  val xR2RML_RRXLISTORMAP_PROPERTY = ResourceFactory.createResource(xR2RML_RRXLISTORMAP_URI);

  //PredicateObjectMap
  val xR2RML_CHILDPARSE_URI = xR2RML_NS + "childParse";
  val xR2RML_CHILDPARSE_PROPERTY = ResourceFactory.createProperty(xR2RML_CHILDPARSE_URI);
  val xR2RML_PARENTPARSE_URI = xR2RML_NS + "parentParse";
  val xR2RML_PARENTPARSE_PROPERTY = ResourceFactory.createProperty(xR2RML_PARENTPARSE_URI);

  // Parse
  val xR2RML_PARSE_URI = xR2RML_NS + "parse";
  val xR2RML_PARSE_PROPERTY = ResourceFactory.createProperty(xR2RML_PARSE_URI);
}