/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.steps.vertexlookup;

import java.math.BigDecimal;
import java.text.DateFormat;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * Looks up information by first reading data into a hash table (in memory)
 * 
 * TODO: add warning with conflicting types OR modify the lookup values to the input row type. (this is harder to do as
 * currently we don't know the types)
 * 
 * @author Matt Burgess
 * @since 28-oct-2013
 */
public class VertexLookup extends BaseStep implements StepInterface {
  private static Class<?> PKG = VertexLookupMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  private VertexLookupMeta meta;
  private VertexLookupData data;

  public VertexLookup( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  private void handleNullIf() {
    data.nullIf = new Object[meta.getValue().length];

    for ( int i = 0; i < meta.getValue().length; i++ ) {
      if ( meta.getValueDefaultType()[i] < 0 ) {
        // logError("unknown default value type: "+dtype+" for value "+value[i]+", default to type: String!");
        meta.getValueDefaultType()[i] = ValueMetaInterface.TYPE_STRING;
      }
      data.nullIf[i] = null;
      switch ( meta.getValueDefaultType()[i] ) {
        case ValueMetaInterface.TYPE_STRING:
          if ( Const.isEmpty( meta.getValueDefault()[i] ) ) {
            data.nullIf[i] = null;
          } else {
            data.nullIf[i] = meta.getValueDefault()[i];
          }
          break;
        case ValueMetaInterface.TYPE_DATE:
          try {
            data.nullIf[i] = DateFormat.getInstance().parse( meta.getValueDefault()[i] );
          } catch ( Exception e ) {
          }
          break;
        case ValueMetaInterface.TYPE_NUMBER:
          try {
            data.nullIf[i] = new Double( Double.parseDouble( meta.getValueDefault()[i] ) );
          } catch ( Exception e ) {
          }
          break;
        case ValueMetaInterface.TYPE_INTEGER:
          try {
            data.nullIf[i] = new Long( Long.parseLong( meta.getValueDefault()[i] ) );
          } catch ( Exception e ) {
          }
          break;
        case ValueMetaInterface.TYPE_BOOLEAN:
          if ( "TRUE".equalsIgnoreCase( meta.getValueDefault()[i] ) || "Y".equalsIgnoreCase( meta.getValueDefault()[i] ) ) {
            data.nullIf[i] = Boolean.TRUE;
          } else {
            data.nullIf[i] = Boolean.FALSE;
          }
          break;
        case ValueMetaInterface.TYPE_BIGNUMBER:
          try {
            data.nullIf[i] = new BigDecimal( meta.getValueDefault()[i] );
          } catch ( Exception e ) {
          }
          break;
        default:
          // if a default value is given and no conversion is implemented throw an error
          if ( meta.getValueDefault()[i] != null && meta.getValueDefault()[i].trim().length() > 0 ) {
            throw new RuntimeException( BaseMessages.getString( PKG, "VertexLookup.Exception.ConversionNotImplemented" )
                + " " + ValueMeta.getTypeDesc( meta.getValueDefaultType()[i] ) );
          } else {
            // no default value given: just set it to null
            data.nullIf[i] = null;
            break;
          }
      }
    }
  }

  private Object[] lookupValues( RowMetaInterface rowMeta, Object[] row ) throws KettleException {
    // See if we need to stop.
    if ( isStopped() ) {
      return null;
    }

    if ( data.lookupColumnIndex == null ) {
      String[] names = data.lookupMeta.getFieldNames();
      data.lookupColumnIndex = new int[names.length];

      for ( int i = 0; i < names.length; i++ ) {
        data.lookupColumnIndex[i] = rowMeta.indexOfValue( names[i] );
        if ( data.lookupColumnIndex[i] < 0 ) {
          // we should not get here
          throw new KettleStepException( "The lookup column '" + names[i] + "' could not be found" );
        }
      }
    }

    // Copy value references to lookup table.
    //
    Object[] lu = new Object[data.keynrs.length];
    for ( int i = 0; i < data.keynrs.length; i++ ) {
      // If the input is binary storage data, we convert it to normal storage.
      //
      if ( data.convertKeysToNative[i] ) {
        lu[i] = data.lookupMeta.getValueMeta( i ).convertBinaryStringToNativeType( (byte[]) row[data.keynrs[i]] );
      } else {
        lu[i] = row[data.keynrs[i]];
      }
    }

    // Handle conflicting types (Number-Integer-String conversion to lookup type in hashtable)
    if ( data.keyTypes != null ) {
      for ( int i = 0; i < data.lookupMeta.size(); i++ ) {
        ValueMetaInterface inputValue = data.lookupMeta.getValueMeta( i );
        ValueMetaInterface lookupValue = data.keyTypes.getValueMeta( i );
        if ( inputValue.getType() != lookupValue.getType() ) {
          try {
            // Change the input value to match the lookup value
            //
            lu[i] = lookupValue.convertDataCompatible( inputValue, lu[i] );
          } catch ( KettleValueException e ) {
            throw new KettleStepException( "Error converting data while looking up value", e );
          }
        }
      }
    }

    Object[] add = null;

    if ( data.hasLookupRows ) {
      try {
        if ( meta.getKeystream().length > 0 ) {
          add = getFromCache( data.keyTypes, lu );
        } else {
          // Just take the first element in the hashtable...
          throw new KettleStepException( BaseMessages.getString( PKG, "VertexLookup.Log.GotRowWithoutKeys" ) );
        }
      } catch ( Exception e ) {
        throw new KettleStepException( e );
      }
    }

    if ( add == null ) // nothing was found, unknown code: add the specified default value...
    {
      add = data.nullIf;
    }

    return RowDataUtil.addRowData( row, rowMeta.size(), add );
  }

  private Object[] getFromCache( RowMetaInterface keyMeta, Object[] keyData ) throws KettleValueException {
    if ( meta.isMemoryPreservationActive() ) {

      try {
        byte[] value = data.hashIndex.get( RowMeta.extractData( keyMeta, keyData ) );
        if ( value == null ) {
          return null;
        }
        return RowMeta.getRow( data.valueMeta, value );
      } catch ( Exception e ) {
        logError( "Oops", e );
        throw new RuntimeException( e );
      }
    } else {
      return data.look.get( new RowMetaAndData( keyMeta, keyData ) );
    }
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (VertexLookupMeta) smi;
    data = (VertexLookupData) sdi;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if ( r == null ) // no more input to be expected...
    {
      if ( log.isDetailed() ) {
        logDetailed( BaseMessages.getString( PKG, "VertexLookup.Log.StoppedProcessingWithEmpty", getLinesRead() + "" ) );
      }
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      // read the lookup values!
      data.keynrs = new int[meta.getKeystream().length];
      data.lookupMeta = new RowMeta();
      data.convertKeysToNative = new boolean[meta.getKeystream().length];

      for ( int i = 0; i < meta.getKeystream().length; i++ ) {
        // Find the keynr in the row (only once)
        data.keynrs[i] = getInputRowMeta().indexOfValue( meta.getKeystream()[i] );
        if ( data.keynrs[i] < 0 ) {
          throw new KettleStepException( BaseMessages.getString( PKG,
              "VertexLookup.Log.FieldNotFound", meta.getKeystream()[i], "" + getInputRowMeta().getString( r ) ) ); //$NON-NLS-3$
        } else {
          if ( log.isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG,
                "VertexLookup.Log.FieldInfo", meta.getKeystream()[i], "" + data.keynrs[i] ) ); //$NON-NLS-3$
          }
        }

        data.lookupMeta.addValueMeta( getInputRowMeta().getValueMeta( data.keynrs[i] ).clone() );

        // If we have binary storage data coming in, we convert it to normal data storage.
        // The storage in the lookup data store is also normal data storage. TODO: enforce normal data storage??
        //
        data.convertKeysToNative[i] = getInputRowMeta().getValueMeta( data.keynrs[i] ).isStorageBinaryString();
      }

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), new RowMetaInterface[] { data.infoMeta }, null, this,
          repository, metaStore );

      // Handle the NULL values (not found...)
      handleNullIf();
    }

    Object[] outputRow = lookupValues( getInputRowMeta(), r ); // Do the actual lookup in the hastable.
    if ( outputRow == null ) {
      setOutputDone(); // signal end to receiver(s)
      return false;
    }

    putRow( data.outputRowMeta, outputRow ); // copy row to output rowset(s);

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "VertexLookup.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (VertexLookupMeta) smi;
    data = (VertexLookupData) sdi;

    if ( super.init( smi, sdi ) ) {
      data.readLookupValues = true;

      return true;
    }
    return false;
  }

  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    // Recover memory immediately, allow in-memory data to be garbage collected
    //
    data.look = null;
    data.hashIndex = null;
    data.longIndex = null;

    super.dispose( smi, sdi );
  }

}
