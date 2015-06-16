package org.pentaho.di.core.row.value;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.SocketTimeoutException;
import java.util.Date;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleEOFException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReader;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;

@ValueMetaPlugin( id = "47274", name = "Graph", description = "A GraphSON representation of a graph" )
public class ValueMetaGraph extends ValueMetaBase {

  public static final int TYPE_GRAPH = 47274; // Value is "GRAPH" on a phone keypad

  public ValueMetaGraph() {
    this( null );
  }

  public ValueMetaGraph( String name ) {
    super( name, TYPE_GRAPH );
  }

  @Override
  public String getString( Object object ) throws KettleValueException {
    /*if ( object instanceof String ) {
      return (String) object;
    }*/
    return convertGraphToString( getGraph( object ) );
  }

  @Override
  public Double getNumber( Object object ) throws KettleValueException {
    throw new KettleValueException( toString() + " : can't be converted to a number" );
  }

  @Override
  public Long getInteger( Object object ) throws KettleValueException {
    throw new KettleValueException( toString() + " : can't be converted to an integer" );
  }

  @Override
  public BigDecimal getBigNumber( Object object ) throws KettleValueException {
    throw new KettleValueException( toString() + " : can't be converted to a big number" );
  }

  @Override
  public Boolean getBoolean( Object object ) throws KettleValueException {
    throw new KettleValueException( toString() + " : can't be converted to a boolean" );
  }

  @Override
  public Date getDate( Object object ) throws KettleValueException {
    throw new KettleValueException( toString() + " : can't be converted to a date" );
  }

  /**
   * Convert the specified data to the data type specified in this object.
   * 
   * @param meta2
   *          the metadata of the object to be converted
   * @param data2
   *          the data of the object to be converted
   * @return the object in the data type of this value metadata object
   * @throws KettleValueException
   *           in case there is a data conversion error
   */
  @Override
  public Object convertData( ValueMetaInterface meta2, Object data2 ) throws KettleValueException {
    switch ( meta2.getType() ) {
      case TYPE_STRING:
        return convertStringToGraph( meta2.getString( data2 ) );
      case TYPE_GRAPH:
        return data2;
      default:
        throw new KettleValueException( meta2.toStringMeta() + " : can't be converted to a Graph" );
    }
  }

  @Override
  public Object getNativeDataType( Object object ) throws KettleValueException {
    return getGraph( object );
  }

  public Graph getGraph( Object object ) throws KettleValueException {
    try {
      if ( object == null ) {
        return null;
      }
      switch ( type ) {
        case TYPE_GRAPH:
          return (Graph) object;
        case TYPE_NUMBER:
          throw new KettleValueException( toString() + " : I don't know how to convert a number to a graph." );
        case TYPE_STRING:
          switch ( storageType ) {
            case STORAGE_TYPE_NORMAL:
              return convertStringToGraph( (String) object );
            case STORAGE_TYPE_BINARY_STRING:
              return convertStringToGraph( (String) convertBinaryStringToNativeType( (byte[]) object ) );
            case STORAGE_TYPE_INDEXED:
              return convertStringToGraph( (String) index[( (Integer) object ).intValue()] );
            default:
              throw new KettleValueException( toString() + " : Unknown storage type " + storageType + " specified." );
          }
        case TYPE_DATE:
          throw new KettleValueException( toString() + " : I don't know how to convert a date to a graph." );
        case TYPE_INTEGER:
          throw new KettleValueException( toString() + " : I don't know how to convert an integer to a graph." );
        case TYPE_BIGNUMBER:
          throw new KettleValueException( toString() + " : I don't know how to convert a big number to a graph." );
        case TYPE_BOOLEAN:
          throw new KettleValueException( toString() + " : I don't know how to convert a boolean to a graph." );
        case TYPE_BINARY:
          throw new KettleValueException( toString() + " : I don't know how to convert a binary value to a graph." );
        case TYPE_SERIALIZABLE:
          throw new KettleValueException( toString()
              + " : I don't know how to convert a serializable value to a graph." );
        default:
          throw new KettleValueException( toString() + " : Unknown type " + type + " specified." );
      }
    } catch ( Exception e ) {
      throw new KettleValueException( "Unexpected conversion error while converting value [" + toString()
          + "] to a GraphSON", e );
    }
  }

  protected Graph convertStringToGraph( String graphString ) throws KettleValueException {
    if ( graphString == null ) {
      return null;
    }
    Graph baseGraph = null;

    try {
      // GraphSON is a string like JSON, so we should just validate the string is GSON-able
      // Before Blueprints 2.4 (which has an ASM conflict with PDI), the only vendor-neutral
      // Graph implementation is the TinkerGraph
      baseGraph = (Graph) new TinkerGraph();

      if ( !Const.isEmpty( graphString ) ) {
        GraphSONReader.inputGraph( baseGraph, new ByteArrayInputStream( graphString.getBytes() ) );
      }
      return baseGraph;
    } catch ( UnsupportedEncodingException uee ) {
      throw new KettleValueException( uee );
    } catch ( IOException ioe ) {
      throw new KettleValueException( ioe );
    }
  }

  protected String convertGraphToString( Graph graph ) throws KettleValueException {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      GraphSONWriter.outputGraph( graph, baos );
      return baos.toString();
    } catch ( Exception e ) {
      throw new KettleValueException( e );
    }
  }

  @Override
  public Object readData( DataInputStream inputStream ) throws KettleFileException, KettleEOFException,
    SocketTimeoutException {
    try {
      // Is the value NULL?
      if ( inputStream.readBoolean() ) {
        return null; // done
      }

      switch ( storageType ) {
        case STORAGE_TYPE_NORMAL:
          // Handle Content -- only when not NULL
          Graph graph = new TinkerGraph();
          GraphSONReader.inputGraph( graph, inputStream );
          return graph;

        case STORAGE_TYPE_BINARY_STRING:
          return readBinaryString( inputStream );

        case STORAGE_TYPE_INDEXED:
          return readSmallInteger( inputStream ); // just an index: 4-bytes should be enough.

        default:
          throw new KettleFileException( toString() + " : Unknown storage type " + getStorageType() );
      }
    } catch ( EOFException e ) {
      throw new KettleEOFException( e );
    } catch ( SocketTimeoutException e ) {
      throw e;
    } catch ( IOException e ) {
      throw new KettleFileException( toString() + " : Unable to read value map data from input stream", e );
    }
  }

  @Override
  public void writeData( DataOutputStream outputStream, Object object ) throws KettleFileException {
    try {
      // Is the value NULL?
      outputStream.writeBoolean( object == null );

      if ( object != null ) {
        switch ( storageType ) {
          case STORAGE_TYPE_NORMAL:
            // Handle Content -- only when not NULL
            GraphSONWriter.outputGraph( (Graph) object, outputStream );
            break;

          case STORAGE_TYPE_BINARY_STRING:
            // Handle binary string content -- only when not NULL
            // In this case, we opt not to convert anything at all for speed.
            // That way, we can save on CPU power.
            // Since the streams can be compressed, volume shouldn't be an issue
            // at all.
            //
            writeBinaryString( outputStream, (byte[]) object );
            break;

          case STORAGE_TYPE_INDEXED:
            writeInteger( outputStream, (Integer) object ); // just an index
            break;

          default:
            throw new KettleFileException( toString() + " : Unknown storage type " + getStorageType() );
        }
      }
    } catch ( ClassCastException e ) {
      throw new RuntimeException( toString() + " : There was a data type error: the data type of "
          + object.getClass().getName() + " object [" + object + "] does not correspond to value meta ["
          + toStringMeta() + "]" );
    } catch ( IOException e ) {
      throw new KettleFileException( toString() + " : Unable to write value graph data to output stream", e );
    }
  }
}
