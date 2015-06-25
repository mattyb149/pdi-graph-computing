/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.addnodestograph;

import java.util.List;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * The Add Nodes To Graph step will add a new vertex to the specified graph and set properties on the vertex as
 * specified by the given Map field
 * 
 */
@Step( id = "AddNodesToGraph", image = "addnodes.png", name = "Add Nodes to Graph",
    description = "Adds nodes with optional property maps to a graph", categoryDescription = "Graph" )
public class AddNodesToGraphMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = AddNodesToGraphMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  private String graphFieldName;
  private String mapFieldName;

  public AddNodesToGraphMeta() {
    super(); // allocate BaseStepMeta
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  public Object clone() {
    AddNodesToGraphMeta retval = (AddNodesToGraphMeta) super.clone();
    retval.setGraphFieldName( this.getGraphFieldName() );
    retval.setMapFieldName( this.getMapFieldName() );
    return retval;
  }

  private void readData( Node stepnode ) throws KettleXMLException {
    try {
      this.setGraphFieldName( XMLHandler.getTagValue( stepnode, "graphfield" ) );
      this.setMapFieldName( XMLHandler.getTagValue( stepnode, "mapfield" ) );
    } catch ( Exception e ) {
      throw new KettleXMLException(
          BaseMessages.getString( PKG, "AddNodesToGraphMeta.Exception.UnableToReadStepInfo" ), e );
    }
  }

  public void setDefault() {
    this.setGraphFieldName( null );
    this.setMapFieldName( null );
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      this.setGraphFieldName( rep.getStepAttributeString( id_step, "graphfield" ) );
      this.setMapFieldName( rep.getStepAttributeString( id_step, "mapfield" ) );

    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "AddNodesToGraphMeta.Exception.UnexpectedErrorReadingStepInfo" ), e );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "graphfield", this.getGraphFieldName() );
      rep.saveStepAttribute( id_transformation, id_step, "mapfield", this.getMapFieldName() );
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "AddNodesToGraphMeta.Exception.UnexpectedErrorSavingStepInfo" ), e );
    }
  }

  public void getFields( RowMetaInterface inputRowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
    /*
     * if ( !Const.isEmpty( this.mapFieldName ) ) { String mapField = ( space == null ) ? this.mapFieldName :
     * space.environmentSubstitute( this.mapFieldName );
     * 
     * // Get class of graph field ValueMetaInterface graphFieldMeta = inputRowMeta.searchValueMeta(
     * this.getGraphFieldName() );
     * 
     * ValueMetaInterface v = new ValueMetaMap( mapField, keyMeta, valueMeta ); v.setOrigin( origin );
     * inputRowMeta.addValueMeta( v ); } else { throw new KettleStepException( BaseMessages.getString( PKG,
     * "AddNodesToGraphMeta.Exception.MapFieldNameNotFound" ) ); }
     */
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String input[], String output[], RowMetaInterface info, VariableSpace space, Repository repository,
      IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString( PKG,
              "AddNodesToGraphMeta.CheckResult.NotReceivingFields" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "AddNodesToGraphMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "AddNodesToGraphMeta.CheckResult.StepRecevingData2" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              "AddNodesToGraphMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
      Trans trans ) {
    return new AddNodesToGraph( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new AddNodesToGraphData();
  }

  public String getMapFieldName() {
    return mapFieldName;
  }

  public void setMapFieldName( String mapFieldName ) {
    this.mapFieldName = mapFieldName;
  }

  @Override
  public String getXML() throws KettleException {
    StringBuffer retval = new StringBuffer();
    retval.append( "    " + XMLHandler.addTagValue( "graphfield", this.getGraphFieldName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "mapfield", this.getMapFieldName() ) );
    return retval.toString();
  }

  public String getGraphFieldName() {
    return graphFieldName;
  }

  public void setGraphFieldName( String graphFieldName ) {
    this.graphFieldName = graphFieldName;
  }

}
