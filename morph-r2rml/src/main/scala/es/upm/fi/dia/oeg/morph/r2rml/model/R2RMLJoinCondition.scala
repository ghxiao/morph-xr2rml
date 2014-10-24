package es.upm.fi.dia.oeg.morph.r2rml.model

import org.apache.log4j.Logger
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class R2RMLJoinCondition(val childColumnName: String, val parentColumnName: String, val childParse: xR2RMLJoinParse, val parentParse: xR2RMLJoinParse) {
  def logger = Logger.getLogger(this.getClass().getName());

  override def toString(): String = {
    return "JOIN(Parent:" + this.parentColumnName + ",CHILD:" + this.childColumnName + ")";

  }
}

object R2RMLJoinCondition {
  def logger = Logger.getLogger(this.getClass().getName());
  def apply(resource: Resource): R2RMLJoinCondition = {
    val childStatement = resource.getProperty(Constants.R2RML_CHILD_PROPERTY);
    if (childStatement == null) {
      val errorMessage = "Missing child column in join condition!";
      logger.error(errorMessage);
      throw new Exception(errorMessage);
    }
    val childColumnName = childStatement.getObject().toString();

    val parentStatement = resource.getProperty(Constants.R2RML_PARENT_PROPERTY);
    if (parentStatement == null) {
      val errorMessage = "Missing parent column in join condition!";
      logger.error(errorMessage);
      throw new Exception(errorMessage);
    }
    val parentColumnName = parentStatement.getObject().toString();

    new R2RMLJoinCondition(childColumnName, parentColumnName, extractChildParse(resource, childColumnName), extractParentParse(resource, parentColumnName));
  }

  /** @note xR2RML */
  def extractChildParse(rdfNode: Resource, joinValue: String): xR2RMLJoinParse = {
    logger.info(" method extractChildParse ");

    var joinParse = rdfNode match {
      case resource: Resource => {

        val termTypeStatement = resource.getProperty(xR2RML_Constants.xR2RML_CHILDPARSE_PROPERTY);
        if (termTypeStatement != null) {

          var parsetype = termTypeStatement.getProperty(xR2RML_Constants.xR2RML_PARSETYPE_PROPERTY).getObject().toString();
          var joinParsef = termTypeStatement.getProperty(xR2RML_Constants.xR2RML_FORMAT_PROPERTY).getObject().toString();
          if (joinParsef == null) {
            joinParsef = xR2RML_Constants.xR2RML_RRXROW_URI
          }
          val temp = new xR2RMLJoinParse(Some(parsetype), joinValue, Some(joinParsef));

          temp
        } else {
          val temp = new xR2RMLJoinParse(Some(Constants.R2RML_LITERAL_URI), joinValue, Some(xR2RML_Constants.xR2RML_RRXROW_URI));
          temp
        }
      }
      case _ => {
        null
      }
    }
    joinParse
  }

  def extractParentParse(rdfNode: Resource, joinValue: String): xR2RMLJoinParse = {
    logger.info(" method extractParentParse ");
    var joinParse = rdfNode match {
      case resource: Resource => {
        val termTypeStatement = resource.getProperty(xR2RML_Constants.xR2RML_PARENTPARSE_PROPERTY);
        if (termTypeStatement != null) {
          var parsetype = termTypeStatement.getProperty(xR2RML_Constants.xR2RML_PARSETYPE_PROPERTY).getObject().toString();
          var joinParsef = termTypeStatement.getProperty(xR2RML_Constants.xR2RML_FORMAT_PROPERTY).getObject().toString();
          if (joinParsef == null) {
            joinParsef = xR2RML_Constants.xR2RML_RRXROW_URI
          }
          val temp = new xR2RMLJoinParse(Some(parsetype), joinValue, Some(joinParsef));
          temp
        } else {
          val temp = new xR2RMLJoinParse(Some(Constants.R2RML_LITERAL_URI), joinValue, Some(xR2RML_Constants.xR2RML_RRXROW_URI));
          temp
        }
      }
      case _ => {
        null
      }
    }

    joinParse
  }

  ////  end of xR2RML

}
