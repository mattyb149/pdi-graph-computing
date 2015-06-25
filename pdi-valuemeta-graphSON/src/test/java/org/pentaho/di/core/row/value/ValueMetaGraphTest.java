package org.pentaho.di.core.row.value;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraphFactory;

public class ValueMetaGraphTest {

  public static final String TINKER_GRAPH_STRING =
      "{\"mode\":\"NORMAL\",\"vertices\":[{\"name\":\"lop\",\"lang\":\"java\",\"_id\":\"3\",\"_type\":\"vertex\"},{\"age\":27,\"name\":\"vadas\",\"_id\":\"2\",\"_type\":\"vertex\"},{\"age\":29,\"name\":\"marko\",\"_id\":\"1\",\"_type\":\"vertex\"},{\"age\":35,\"name\":\"peter\",\"_id\":\"6\",\"_type\":\"vertex\"},{\"name\":\"ripple\",\"lang\":\"java\",\"_id\":\"5\",\"_type\":\"vertex\"},{\"age\":32,\"name\":\"josh\",\"_id\":\"4\",\"_type\":\"vertex\"}],\"edges\":[{\"weight\":1.0,\"_id\":\"10\",\"_type\":\"edge\",\"_outV\":\"4\",\"_inV\":\"5\",\"_label\":\"created\"},{\"weight\":0.5,\"_id\":\"7\",\"_type\":\"edge\",\"_outV\":\"1\",\"_inV\":\"2\",\"_label\":\"knows\"},{\"weight\":0.4,\"_id\":\"9\",\"_type\":\"edge\",\"_outV\":\"1\",\"_inV\":\"3\",\"_label\":\"created\"},{\"weight\":1.0,\"_id\":\"8\",\"_type\":\"edge\",\"_outV\":\"1\",\"_inV\":\"4\",\"_label\":\"knows\"},{\"weight\":0.4,\"_id\":\"11\",\"_type\":\"edge\",\"_outV\":\"4\",\"_inV\":\"3\",\"_label\":\"created\"},{\"weight\":0.2,\"_id\":\"12\",\"_type\":\"edge\",\"_outV\":\"6\",\"_inV\":\"3\",\"_label\":\"created\"}]}";

  public static final String EMPTY_GRAPH_STRING = "{\"mode\":\"NORMAL\",\"vertices\":[],\"edges\":[]}";

  protected static Graph GRAPH = null;

  @Before
  public void setUp() throws Exception {
    GRAPH = new TinkerGraph();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testValueMetaGraph() {
    ValueMetaGraph vmg = new ValueMetaGraph();
    assertNull( vmg.getName() );
  }

  @Test
  public void testValueMetaGraphString() {
    final String name = "GRAPH_NAME";
    ValueMetaGraph vmg = new ValueMetaGraph( name );
    assertEquals( vmg.getName(), name );
  }

  @Test( expected = KettleValueException.class )
  public void testGetStringObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getString( "test" );
  }

  @Test( expected = KettleValueException.class )
  public void testGetStringNullObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getString( null );
  }

  @Test( expected = KettleValueException.class )
  public void testGetNumberObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getNumber( "test" );
  }

  @Test( expected = KettleValueException.class )
  public void testGetNumberNullObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getNumber( null );
  }

  @Test( expected = KettleValueException.class )
  public void testGetIntegerObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getInteger( "test" );
  }

  @Test( expected = KettleValueException.class )
  public void testGetIntegerNullObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getInteger( null );
  }

  @Test( expected = KettleValueException.class )
  public void testGetBigNumberObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getBigNumber( "test" );
  }

  @Test( expected = KettleValueException.class )
  public void testGetBigNumberNullObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getBigNumber( null );
  }

  @Test( expected = KettleValueException.class )
  public void testGetBooleanObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getBoolean( "test" );
  }

  @Test( expected = KettleValueException.class )
  public void testGetBooleanNullObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getBoolean( null );
  }

  @Test( expected = KettleValueException.class )
  public void testGetDateObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getDate( new Date() );
  }

  @Test( expected = KettleValueException.class )
  public void testGetDateNullObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getDate( null );
  }

  @Test
  public void testConvertDataNullStringToGraph() throws KettleValueException {
    ValueMetaInterface vmi = new ValueMetaString();
    String graphString = null;

    ValueMetaGraph vmg = new ValueMetaGraph();
    Object convertedData = vmg.convertData( vmi, graphString );
    assertNull( convertedData );
  }

  @Test
  public void testConvertDataEmptyStringToGraph() throws KettleValueException {
    ValueMetaInterface vmi = new ValueMetaString();
    String graphString = "";

    ValueMetaGraph vmg = new ValueMetaGraph();
    Object convertedData = vmg.convertData( vmi, graphString );
    assertNotNull( convertedData );
    assertTrue( convertedData instanceof Graph );
    Graph g = (Graph) convertedData;
    assertEquals( 0, getNumberOfVertices( g ) );
  }

  @Test
  public void testConvertDataTinkerStringToGraph() throws KettleValueException {
    ValueMetaInterface vmi = new ValueMetaString();
    String graphString = TINKER_GRAPH_STRING;

    ValueMetaGraph vmg = new ValueMetaGraph();
    Object convertedData = vmg.convertData( vmi, graphString );
    assertNotNull( convertedData );
    assertTrue( convertedData instanceof Graph );
    Graph g = (Graph) convertedData;
    assertEquals( 6, getNumberOfVertices( g ) );
  }

  @Test
  public void testConvertDataGraphToGraph() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    Object convertedData = vmg.convertData( vmg, GRAPH );
    assertNotNull( convertedData );
    assertTrue( convertedData instanceof Graph );
    Graph g = (Graph) convertedData;
    assertEquals( 0, getNumberOfVertices( g ) );
  }

  @Test( expected = KettleValueException.class )
  public void testConvertDataIntegerToGraph() throws KettleValueException {
    ValueMetaInterface vmi = new ValueMetaInteger();
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.convertData( vmi, 14 );
    fail( "Should not be able to convert Graph to Integer!" );
  }

  @Test
  public void testGetNativeDataTypeObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    Graph graph = new TinkerGraph();
    Object nativeData = vmg.getNativeDataType( graph );
    assertTrue( nativeData instanceof Graph );
    assertEquals( graph, (Graph) nativeData );
  }

  @Test( expected = KettleValueException.class )
  public void testGetGraphIntegerObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getGraph( 14 );
    fail( "Should not be able to convert Integer to Graph!" );
  }

  @Test( expected = KettleValueException.class )
  public void testGetGraphBooleanObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.getGraph( true );
    fail( "Should not be able to convert Boolean to Graph!" );
  }

  @Test( expected = KettleValueException.class )
  public void testGetGraphEmptyStringObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    assertNotNull( vmg.getGraph( "" ) );
  }

  @Test
  public void testGetGraphNullObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    assertNull( vmg.getGraph( null ) );
  }

  @Test
  public void testGetGraphGraphObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    Graph g = new TinkerGraph();
    assertEquals( g, vmg.getGraph( g ) );
  }

  @Test
  public void testConvertStringToGraphNullString() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    assertNull( vmg.convertStringToGraph( null ) );
  }

  @Test
  public void testConvertStringToGraphEmptyString() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    Graph g = vmg.convertStringToGraph( "" );
    assertNotNull( g );
    Iterable<Vertex> vertices = g.getVertices();
    assertNotNull( vertices );
    assertEquals( 0, getNumberOfVertices( g ) );
  }

  @Test
  public void testConvertStringToGraphTinkerGraphString() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    Graph g = vmg.convertStringToGraph( TINKER_GRAPH_STRING );
    assertNotNull( g );
    Iterable<Vertex> vertices = g.getVertices();
    assertNotNull( vertices );

    assertEquals( 6, getNumberOfVertices( g ) );
  }

  @Test( expected = KettleValueException.class )
  public void testConvertGraphToStringNullObject() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    vmg.convertGraphToString( null );
  }

  @Test
  public void testConvertGraphToStringEmptyGraph() throws KettleValueException {
    ValueMetaGraph vmg = new ValueMetaGraph();
    String graphString = vmg.convertGraphToString( new TinkerGraph() );
    assertEquals( EMPTY_GRAPH_STRING, graphString );
  }

  @Test
  public void testConvertGraphToStringTinkerGraph() throws KettleValueException {
    Graph tg = TinkerGraphFactory.createTinkerGraph();

    ValueMetaGraph vmg = new ValueMetaGraph();
    String graphString = vmg.convertGraphToString( tg );
    assertNotNull( graphString );
    assertEquals( graphString, TINKER_GRAPH_STRING );
  }

  protected int getNumberOfVertices( Graph g ) {

    int count = 0;
    for ( @SuppressWarnings( "unused" )
    Vertex v : g.getVertices() ) {
      count++;
    }

    return count;
  }

  protected int getNumberOfEdges( Graph g ) {

    int count = 0;
    for ( @SuppressWarnings( "unused" )
    Edge e : g.getEdges() ) {
      count++;
    }

    return count;
  }

}
