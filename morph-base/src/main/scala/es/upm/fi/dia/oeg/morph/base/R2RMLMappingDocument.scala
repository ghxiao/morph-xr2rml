package es.upm.fi.dia.oeg.morph.base

import scala.collection.JavaConversions._
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.rdf.model.Resource
import org.apache.log4j.Logger

class R2RMLMappingDocument(mappingFile: String) {
  val logger = Logger.getLogger(this.getClass());

  val XSDIntegerURI = XSDDatatype.XSDinteger.getURI();
  val XSDDoubleURI = XSDDatatype.XSDdouble.getURI();
  val XSDDateTimeURI = XSDDatatype.XSDdateTime.getURI();
  var datatypesMap: Map[String, String] = Map();
  datatypesMap += ("INT" -> XSDIntegerURI);
  datatypesMap += ("DOUBLE" -> XSDDoubleURI);
  datatypesMap += ("DATETIME" -> XSDDateTimeURI);

  val model = ModelFactory.createDefaultModel();
  val in = FileManager.get().open(mappingFile);
  model.read(in, null, "TURTLE");

  def getTriplesMapResourcesByRRClass(classURI: String) = {

    var result: List[Resource] = Nil;
    logger.info(" method getTriplesMapResourcesByRRClass ");
    val triplesMapResources = model.listSubjectsWithProperty(Constants.R2RML_SUBJECTMAP_PROPERTY);
    if (triplesMapResources != null) {
      val triplesMapsList = triplesMapResources.toList();
      for (triplesMapResource <- triplesMapsList) yield {
        val subjectMapResource = triplesMapResource.getPropertyResourceValue(Constants.R2RML_SUBJECTMAP_PROPERTY);
        if (subjectMapResource != null) {
          val rrClassResource = subjectMapResource.getPropertyResourceValue(Constants.R2RML_CLASS_PROPERTY);
          if (rrClassResource != null) {
            val subjectMapClassURI = rrClassResource.getURI();
            if (classURI.equals(subjectMapClassURI)) {
              result = result ::: result ::: List(triplesMapResource);
            }
          }
        }
      }
    }

    result;
  }

  def getPredicateObjectMapResources(triplesMapResources: List[Resource]): List[Resource] = {

    val result = {
      if (triplesMapResources.isEmpty) {
        Nil; ;
      } else {
        val resultHead = this.getPredicateObjectMapResources(triplesMapResources.head);
        val resultTail = this.getPredicateObjectMapResources(triplesMapResources.tail);
        resultHead ::: resultHead ::: resultTail;
      }
    }
    result;
  }

  def getPredicateObjectMapResources(triplesMapResource: Resource): List[Resource] = {

    val predicateObjectMapStatementsIterator = triplesMapResource.listProperties(Constants.R2RML_PREDICATEOBJECTMAP_PROPERTY);
    val predicateObjectMapStatements = predicateObjectMapStatementsIterator.toList().toList;
    for (predicateObjectMapResource <- predicateObjectMapStatements) {
      val predicateObjectMapResourceObject = predicateObjectMapResource.getObject();
      val predicateObjectMapResourceObjectResource = predicateObjectMapResourceObject.asResource();
    }

    logger.info(" method getPredicateObjectMapResources ");
    val result = for { predicateObjectMapStatement <- predicateObjectMapStatements }
      yield predicateObjectMapStatement.getObject().asResource();
    result;
  }

  def getRRLogicalTable(triplesMapResource: Resource) = {
    val rrLogicalTableResource = triplesMapResource.getPropertyResourceValue(Constants.R2RML_LOGICALTABLE_PROPERTY);
    rrLogicalTableResource;
  }

  /// xR2RML
  def getRRLogicalSource(triplesMapResource: Resource) = {
    val rrLogicalSourceResource = triplesMapResource.getPropertyResourceValue(xR2RML_Constants.xR2RML_LOGICALSOURCE_PROPERTY);
    rrLogicalSourceResource;
  }

  def getRRLogicalSourceSourceName(triplesMapResource: Resource) = {

    logger.info(" method getRRLogicalSourceSourceName ");
    val rrLogicalSourceResource = this.getRRLogicalSource(triplesMapResource);
    val rrSourceNameResource = rrLogicalSourceResource.getPropertyResourceValue(xR2RML_Constants.xR2RML_SOURCENAME_PROPERTY);
    val result = {
      if (rrSourceNameResource != null) {
        val sourceName = rrSourceNameResource.asLiteral().getValue().toString();
        sourceName;
      } else {
        null;
      }
    }
    result;
  }

  def getRRLogicalSourceFormat(triplesMapResource: Resource) = {

    val rrLogicalSourceResource = this.getRRLogicalSource(triplesMapResource);
    val rrFormatResource = rrLogicalSourceResource.getPropertyResourceValue(xR2RML_Constants.xR2RML_FORMAT_PROPERTY);
    val result = {
      if (rrFormatResource != null) {
        val format = rrFormatResource.asLiteral().getValue().toString();
        format;
      } else {
        null;
      }
    }
    result;
  }
  ///////////////// end of xR2RML

  def getRRSubjectMapResource(triplesMapResource: Resource) = {

    val rrSubjectMapResource = triplesMapResource.getPropertyResourceValue(Constants.R2RML_SUBJECTMAP_PROPERTY);
    rrSubjectMapResource;
  }

  def getRRLogicalTableTableName(triplesMapResource: Resource) = {

    val rrLogicalTableResource = this.getRRLogicalTable(triplesMapResource);
    val rrTableNameResource = rrLogicalTableResource.getPropertyResourceValue(Constants.R2RML_TABLENAME_PROPERTY);
    val result = {
      if (rrTableNameResource != null) {
        val tableName = rrTableNameResource.asLiteral().getValue().toString();
        tableName;
      } else {
        null;
      }
    }
    result;
  }

  def getObjectMapResource(predicateObjectMapResource: Resource) = {

    val objectMapResource = predicateObjectMapResource.getPropertyResourceValue(Constants.R2RML_OBJECTMAP_PROPERTY);
    val parentTriplesMap = objectMapResource.getPropertyResourceValue(Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
    val result: Resource = {
      if (parentTriplesMap == null) {
        objectMapResource
      } else {
        null;
      }
    }

    result;
  }

  def getRefObjectMapResource(predicateObjectMapResource: Resource) = {
    val objectMapResource = predicateObjectMapResource.getPropertyResourceValue(Constants.R2RML_OBJECTMAP_PROPERTY);
    val parentTriplesMap = objectMapResource.getPropertyResourceValue(Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
    val result: Resource = {
      if (parentTriplesMap != null) {
        objectMapResource
      } else {
        null;
      }
    }

    result;
  }

  def getParentTriplesMapResource(objectMapResource: Resource) = {
    val parentTriplesMapResource = objectMapResource.getPropertyResourceValue(Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
    parentTriplesMapResource;
  }

  def getParentTriplesMapLogicalTableResource(objectMapResource: Resource) = {
    logger.info(" method getParentTriplesMapLogicalTableResource ");
    val parentTriplesMapResource = this.getParentTriplesMapResource(objectMapResource);
    val parentTriplesMapLogicalTableResource = this.getRRLogicalTable(Constants.R2RML_LOGICALTABLE_PROPERTY);
    parentTriplesMapLogicalTableResource
  }

  def getTermTypeResource(termMapResource: Resource) = {
    val termTypeResource = termMapResource.getPropertyResourceValue(Constants.R2RML_TERMTYPE_PROPERTY);
    // xR2RML
    val xtermTypeResource = termMapResource.getPropertyResourceValue(xR2RML_Constants.xR2RML_TERMTYPE_PROPERTY);
    if (xtermTypeResource != null) {
      xtermTypeResource
    } else {
      termTypeResource;
    }
  }

  def getRRColumnResource(termMapResource: Resource) = {
    logger.info(" method getRRColumnResource ");
    val rrColumnResource = termMapResource.getProperty(Constants.R2RML_COLUMN_PROPERTY);
    rrColumnResource;
  }

  def getRRTemplateResource(termMapResource: Resource) = {
    logger.info(" method getRRTemplateResource ");
    val rrTemplateResource = termMapResource.getProperty(Constants.R2RML_TEMPLATE_PROPERTY);
    rrTemplateResource.getObject();
  }

  def getDatatypeResource(termMapResource: Resource) = {
    logger.info(" method getDatatypeResource ");
    val datatypeResource = termMapResource.getPropertyResourceValue(Constants.R2RML_DATATYPE_PROPERTY);
    datatypeResource;
  }

  def getTermMapType(termMapResource: Resource) = {
    logger.info(" method getTermMapType ");
    if (termMapResource == null) {
      logger.info("termMapResource is null");
    }

    val props = termMapResource.listProperties().toList();

    val termMapType = {
      val constantResource = termMapResource.getProperty(Constants.R2RML_CONSTANT_PROPERTY);
      if (constantResource != null) {
        Constants.MorphTermMapType.ConstantTermMap;
      } else {
        val columnResource = termMapResource.getProperty(Constants.R2RML_COLUMN_PROPERTY);
        if (columnResource != null) {
          Constants.MorphTermMapType.ColumnTermMap;
        } else {
          val templateResource = termMapResource.getProperty(Constants.R2RML_TEMPLATE_PROPERTY);
          if (templateResource != null) {
            Constants.MorphTermMapType.TemplateTermMap;
          } else {
            val referenceResource = termMapResource.getProperty(xR2RML_Constants.xR2RML_REFERENCE_PROPERTY);
            if (referenceResource != null) {
              Constants.MorphTermMapType.ReferenceTermMap
            } else {
              Constants.MorphTermMapType.InvalidTermMapType;
            }
          }
        }
      }
    }
    termMapType;
  }

  def getTemplateValues(termMapResource: Resource, uri: String): Map[String, String] = {
    val termMapValueType = this.getTermMapType(termMapResource);

    val result: Map[String, String] = {
      if (termMapValueType == Constants.MorphTermMapType.TemplateTermMap) {
        val templateString = this.getRRTemplateResource(termMapResource).asLiteral().getValue().toString();
        val matchedTemplate = RegexUtility.getTemplateMatching(templateString, uri);
        matchedTemplate.toMap;
      } else {
        Map();
      }
    }

    result;
  }

}