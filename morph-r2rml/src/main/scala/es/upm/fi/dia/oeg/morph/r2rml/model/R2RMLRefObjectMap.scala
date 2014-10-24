package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class R2RMLRefObjectMap(val parentTriplesMapResource: Resource, val joinConditions: Set[R2RMLJoinCondition], termType: Option[String]) {

  val logger = Logger.getLogger(this.getClass().getName());
  var rdfNode: RDFNode = null;

  val termt = getReftermType;

  if (termt != xR2RML_Constants.xR2RML_RDFALT_URI && termt != xR2RML_Constants.xR2RML_RDFSEQ_URI
    && termt != xR2RML_Constants.xR2RML_RDFBAG_URI && termt != xR2RML_Constants.xR2RML_RDFLIST_URI
    && termt != xR2RML_Constants.xR2RML_RDFTRIPLES_URI) {
    throw new Exception("Illegal termtype in refObjectMap ");
  }

  def getRelationName() = this.rdfNode.asResource().getLocalName();
  def getRangeClassMapping() = this.getParentTripleMapName;

  def getJoinConditions(): java.util.Collection[R2RMLJoinCondition] = {
    joinConditions;
  }

  def getParentTripleMapName(): String = {
    this.parentTriplesMapResource.getURI();
  }

  def getReftermType(): String = {
    if (this.termType.isDefined) {
      this.termType.get
    } else {
      xR2RML_Constants.xR2RML_RDFTRIPLES_URI
    }

  }

  override def toString(): String = {
    this.parentTriplesMapResource.toString();
  }

}

object R2RMLRefObjectMap {
  val logger = Logger.getLogger(this.getClass().getName());

  def apply(resource: Resource): R2RMLRefObjectMap = {
    val parentTriplesMapStatement = resource.getProperty(
      Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);

    val parentTriplesMap = if (parentTriplesMapStatement != null) {
      parentTriplesMapStatement.getObject().asInstanceOf[Resource];
    } else {
      null
    }

    val joinConditionsStatements = resource.listProperties(
      Constants.R2RML_JOINCONDITION_PROPERTY);
    val joinConditions: Set[R2RMLJoinCondition] = if (joinConditionsStatements != null) {
      joinConditionsStatements.map(joinConditionStatement => {
        val joinConditionResource = joinConditionStatement.getObject().asInstanceOf[Resource];
        val joinCondition = R2RMLJoinCondition(joinConditionResource);
        joinCondition
      }).toSet
    } else {
      val errorMessage = "No join conditions defined!";
      logger.warn(errorMessage);
      Set.empty;
    }

    val rom = new R2RMLRefObjectMap(parentTriplesMap, joinConditions, extractTermType(resource));
    //		rom.resource = resource;
    rom
  }

  // xR2RML

  def extractTermType(rdfNode: RDFNode) = {

    logger.info(" method extractTermType ");
    rdfNode match {
      case resource: Resource => {
        val termTypeStatement = resource.getProperty(Constants.R2RML_TERMTYPE_PROPERTY);
        if (termTypeStatement != null) {
          Some(termTypeStatement.getObject().toString());
        } else {
          None
        }
      }
      case _ => {
        None
      }
    }

  }
  ////  end of xR2RML

  def isRefObjectMap(resource: Resource): Boolean = {
    val parentTriplesMapStatement = resource.getProperty(
      Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
    val hasParentTriplesMap = if (parentTriplesMapStatement != null) {
      true;
    } else {
      false
    }
    hasParentTriplesMap;
  }

  def extractRefObjectMaps(resource: Resource): Set[R2RMLRefObjectMap] = {
    val mappingProperties = List(Constants.R2RML_OBJECTMAP_PROPERTY);
    val maps = mappingProperties.map(mapProperty => {
      val mapStatements = resource.listProperties(mapProperty);
      if (mapStatements != null) {
        mapStatements.toList().flatMap(mapStatement => {
          if (mapStatement != null) {
            val mapStatementObject = mapStatement.getObject();
            val mapStatementObjectResource = mapStatementObject.asInstanceOf[Resource];
            if (R2RMLRefObjectMap.isRefObjectMap(mapStatementObjectResource)) {
              val rom = R2RMLRefObjectMap(mapStatementObjectResource);
              rom.rdfNode = mapStatementObjectResource;
              Some(rom);
            } else {
              None;
            }
          } else {
            None
          }
        });
      } else {
        Nil
      }
    }).flatten
    maps.toSet;
  }
}