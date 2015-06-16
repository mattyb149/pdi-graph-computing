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

import java.lang.reflect.Method;
import java.util.Map;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.value.ValueMetaGraph;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

/**
 * The Add Nodes to Graph step will take key/value pairs from the input rows and join them into a single map field
 * 
 */
public class AddNodesToGraph extends BaseStep implements StepInterface {
  private static Class<?> PKG = AddNodesToGraphMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  private AddNodesToGraphMeta meta;
  private AddNodesToGraphData data;

  public AddNodesToGraph( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    return super.init( smi, sdi );
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (AddNodesToGraphMeta) smi;
    data = (AddNodesToGraphData) sdi;

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) // no more input to be expected...
    {

      setOutputDone();
      return false;
    }

    // Output row
    data.outputRowMeta = getInputRowMeta().clone();

    // Get output fields
    meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );

    // Check for Graph field
    final int graphFieldIndex = data.outputRowMeta.indexOfValue( meta.getGraphFieldName() );
    if ( graphFieldIndex < 0 ) {
      logError( BaseMessages.getString( PKG, "AddNodesToGraph.Error.NotFound.GraphField" ) );
      setErrors( 1L );
      setOutputDone();
      return false;
    }

    // Check for node properties field
    final int mapFieldIndex = data.outputRowMeta.indexOfValue( meta.getMapFieldName() );
    if ( mapFieldIndex < 0 ) {
      logError( BaseMessages.getString( PKG, "AddNodesToGraph.Error.NotFound.NodeMapField" ) );
      setErrors( 1L );
      setOutputDone();
      return false;
    }

    ValueMetaGraph vmg = (ValueMetaGraph) data.outputRowMeta.getValueMeta( graphFieldIndex );
    Graph g = vmg.getGraph( r[graphFieldIndex] );

    // Need to use reflection here, Kettle plugins are isolated
    Method findPluginWithName = null;
    Object pluginRegistry = null;
    Method getClassLoader = null;
    try {
      // Get the plugin via reflection on the PluginRegistry
      Class<?> pluginRegistryClass = Class.forName( "org.pentaho.di.core.plugins.PluginRegistry" );
      Class<?> pluginInterfaceClass = Class.forName( "org.pentaho.di.core.plugins.PluginInterface" );
      Method getInstance = pluginRegistryClass.getMethod( "getInstance" );
      getClassLoader = pluginRegistryClass.getMethod( "getClassLoader", pluginInterfaceClass );
      findPluginWithName = pluginRegistryClass.getMethod( "findPluginWithName", Class.class, String.class );
      pluginRegistry = getInstance.invoke( pluginRegistryClass );
      Class<?> valueMetaPluginTypeClass = Class.forName( "org.pentaho.di.core.row.value.ValueMetaPluginType" );
      Object valueMetaMapPlugin = findPluginWithName.invoke( pluginRegistry, valueMetaPluginTypeClass, "Map" );

      // Get the classloader for the plugin, then fetch the ValueMetaMap class from it
      ClassLoader cl = (ClassLoader) getClassLoader.invoke( pluginRegistry, valueMetaMapPlugin );
      Class<?> valueMetaMapClass = Class.forName( "org.pentaho.di.core.row.value.ValueMetaMap", true, cl );

      // Cast the incoming object to a ValueMetaMap
      Object vmm = valueMetaMapClass.cast( data.outputRowMeta.getValueMeta( mapFieldIndex ) );
      Method getMap = valueMetaMapClass.getMethod( "getMap", Object.class );

      // Add a vertex, then set the given properties for it
      @SuppressWarnings( "unchecked" )
      Map<Object, Object> map = (Map<Object, Object>) getMap.invoke( vmm, r[mapFieldIndex] );
      Vertex v = g.addVertex( null );
      for ( Map.Entry<Object, Object> entry : map.entrySet() ) {
        v.setProperty( entry.getKey().toString(), entry.getValue() );
      }
    } catch ( Exception e ) {
      throw new KettleException( e );
    }

    // Set the graph in the output row just in case
    r[graphFieldIndex] = g;

    putRow( data.outputRowMeta, r ); // copy row to possible alternate rowset(s).

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() )
        logBasic( BaseMessages.getString( PKG, "AddNodesToGraph.Log.LineNumber" ) + getLinesRead() );
    }

    return true;
  }
}
