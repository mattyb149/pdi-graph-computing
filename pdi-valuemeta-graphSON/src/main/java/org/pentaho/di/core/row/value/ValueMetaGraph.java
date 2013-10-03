package org.pentaho.di.core.row.value;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.Date;

import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraphFactory;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReader;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;


@ValueMetaPlugin(id="47274", name="Graph", description="A GraphSON representation of a graph")
public class ValueMetaGraph extends ValueMetaBase {
  
  public static final int TYPE_GRAPH = 47274;  // Value is "GRAPH" on a phone keypad
  
  public ValueMetaGraph() {
    this(null);
  }
  
  public ValueMetaGraph(String name) {
    super(name, TYPE_GRAPH);
  }
  
  @Override
  public String getString(Object object) throws KettleValueException {
    return convertGraphToString(getGraph(object));
  }

  @Override
  public Double getNumber(Object object) throws KettleValueException {
    throw new KettleValueException(toString()+" : can't be converted to a number");
  }

  @Override
  public Long getInteger(Object object) throws KettleValueException {
    throw new KettleValueException(toString()+" : can't be converted to an integer");
  }

  @Override
  public BigDecimal getBigNumber(Object object) throws KettleValueException {
    throw new KettleValueException(toString()+" : can't be converted to a big number");
  }

  @Override
  public Boolean getBoolean(Object object) throws KettleValueException {
    throw new KettleValueException(toString()+" : can't be converted to a boolean");
  }

  @Override
  public Date getDate(Object object) throws KettleValueException {
    throw new KettleValueException(toString()+" : can't be converted to a date");
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
  public Object convertData(ValueMetaInterface meta2, Object data2) throws KettleValueException {
    switch(meta2.getType()) {
    case TYPE_STRING: return convertStringToGraph(meta2.getString(data2)); 
    case TYPE_GRAPH: return data2;
    default: 
      throw new KettleValueException(meta2.toStringMeta()+" : can't be converted to a Graph");
    }
  }
  
  @Override
  public Object getNativeDataType(Object object) throws KettleValueException {
    return getGraph(object);
  }
  
  public Graph getGraph(Object object) throws KettleValueException {
    try {
      if (object == null) {
        return null;
      }
      switch (type) {
      case TYPE_GRAPH:
        return (Graph)object;
      case TYPE_NUMBER:
        throw new KettleValueException(toString() + " : I don't know how to convert a number to a graph.");
      case TYPE_STRING:
        switch (storageType) {
        case STORAGE_TYPE_NORMAL:
          return convertStringToGraph((String) object);
        case STORAGE_TYPE_BINARY_STRING:
          return convertStringToGraph((String) convertBinaryStringToNativeType((byte[]) object));
        case STORAGE_TYPE_INDEXED:
          return convertStringToGraph((String) index[((Integer) object).intValue()]);
        default:
          throw new KettleValueException(toString() + " : Unknown storage type " + storageType + " specified.");
        }
      case TYPE_DATE:
        throw new KettleValueException(toString() + " : I don't know how to convert a date to a graph.");
      case TYPE_INTEGER:
        throw new KettleValueException(toString() + " : I don't know how to convert an integer to a graph.");
      case TYPE_BIGNUMBER:
        throw new KettleValueException(toString() + " : I don't know how to convert a big number to a graph.");
      case TYPE_BOOLEAN:
        throw new KettleValueException(toString() + " : I don't know how to convert a boolean to a graph.");
      case TYPE_BINARY:
        throw new KettleValueException(toString() + " : I don't know how to convert a binary value to a graph.");
      case TYPE_SERIALIZABLE:
        throw new KettleValueException(toString() + " : I don't know how to convert a serializable value to a graph.");
      default:
        throw new KettleValueException(toString() + " : Unknown type " + type + " specified.");
      }
    } catch (Exception e) {
      throw new KettleValueException("Unexpected conversion error while converting value [" + toString()
          + "] to a GraphSON", e);
    }
  }

  protected Graph convertStringToGraph(String graphString) throws KettleValueException {
    if(graphString == null) {
      return null;
    }
    Graph baseGraph = null;
    
    try {
      // GraphSON is a string like JSON, so we should just validate the string is GSON-able
      //  Before Blueprints 2.4 (which has an ASM conflict with PDI), the only vendor-neutral
      //  Graph implementation is the TinkerGraph, which has a toy problem built-in. We need to 
      //  represent just the JSON, so create the toy problem and clear out the object.
      // TODO: There's gotta be a better way
      baseGraph = (Graph)TinkerGraphFactory.createTinkerGraph();
      for(Edge e : baseGraph.getEdges()) {
        baseGraph.removeEdge(e);
      }
      for(Vertex v : baseGraph.getVertices()) {
        baseGraph.removeVertex(v);
      }
      
      GraphSONReader.inputGraph(baseGraph, new ByteArrayInputStream(graphString.getBytes()));
      return baseGraph;
    }
    catch(UnsupportedEncodingException uee) {
      throw new KettleValueException(uee);
    }
    catch(IOException ioe) {
      throw new KettleValueException(ioe);
    }
  }
  
  protected String convertGraphToString(Graph graph) throws KettleValueException {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      GraphSONWriter.outputGraph(graph, baos);
      return baos.toString();
    }
    catch(Exception e) {
      throw new KettleValueException(e);
    }    
  }
}
