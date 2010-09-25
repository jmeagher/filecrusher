/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package crush;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author edward
 */
@SuppressWarnings("deprecation")
public class CrushTest {

    private JobConf jobConf;
    private FileSystem fs;
    private String tmpRoot;
    private Path tmpRootPath;

    public CrushTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {

    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
        jobConf= new JobConf(Crush.class);
        try{
            fs = FileSystem.get(jobConf);
            tmpRoot = "/tmp/" + System.getProperty("user.name") + "/"+System.currentTimeMillis();
            tmpRootPath = new Path(tmpRoot);
            if (fs.exists(tmpRootPath)){
                fs.delete(tmpRootPath, true);
            }
            fs.mkdirs( new Path(tmpRoot) );
            Path aFolder = new Path(tmpRootPath,"afolder");
            fs.mkdirs( aFolder );
            fs.mkdirs( new Path (aFolder,"asub1"));
            fs.mkdirs( new Path (aFolder,"asub2"));
            fs.mkdirs( new Path(tmpRootPath,"bfolder"));
        } catch (Exception ex){
            fail("Could not get FileSystem");
        }
    }

    @After
    public void tearDown() {
        try{
            fs.delete(tmpRootPath, true);
            fs.close();
        } catch (Exception ex){
            fail("Could not tear down FileSystem");
        }
    }

    @Test
    public void testNamePolicy() throws Exception{
        String name="act-001084-20100630-FO_20100630004935-part-00014";
        CrushMapper m = new CrushMapper();
        Date d = new java.util.Date();
        d.setTime(1278014211624L);
        String res = m.buildOutputFileName(name, d);
        assertEquals("act-001084-20100630-CR_20100701155651" , res);

    }
    
    @Test
    public void testCompactText() throws Exception{
        String cloneRoot = "file:/tmp/" + System.getProperty("user.name") + "/clone"+System.currentTimeMillis();
        Path clonePath = new Path(cloneRoot);
        fs.mkdirs(clonePath);

        Crush instance = new Crush();
        instance.setSourcePath(tmpRoot+"/afolder/asub1");
        instance.setClonePath(cloneRoot);
        instance.setCrushType("TEXT");
        
        //we need to generate some text files here
        Path aFile = new Path(this.tmpRootPath,"afolder/asub1/file1");
        FSDataOutputStream out = fs.create(aFile);
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter (out ) );
		out.write( "1".getBytes()  );
		out.write( "\t".getBytes() );
		out.write( "ed".getBytes() );
		out.write( "\n".getBytes() );

		out.write( "2".getBytes()  );
		out.write( "\t".getBytes() );
		out.write( "bob".getBytes() );
		out.write( "\n".getBytes() );

		out.write( "3".getBytes()  );
		out.write( "\t".getBytes() );
		out.write( "derek".getBytes() );
		out.write( "\n".getBytes() );
		br.close();
		

		Path bFile = new Path(this.tmpRootPath,"afolder/asub1/file2");
		FSDataOutputStream out2 = fs.create(bFile);
		BufferedWriter br2 = new BufferedWriter( new OutputStreamWriter (out2 ) );
		out2.write( "6".getBytes()  );
		out2.write( "\t".getBytes() );
		out2.write( "john".getBytes() );
		out2.write( "\n".getBytes() );

		out2.write( "7".getBytes()  );
		out2.write( "\t".getBytes() );
		out2.write( "suzan".getBytes() );
		out2.write( "\n".getBytes() );
		br2.close();
	
		instance.setConf(jobConf);
        instance.compact();
        Path aCrushedPath = new Path(this.tmpRootPath,"afolder/asub1");
        FileStatus [] subFiles = fs.listStatus(aCrushedPath);
        assertEquals(1,  subFiles.length);
        Path aCrushedFile = subFiles[0].getPath();
        BufferedReader resultReader = new BufferedReader(new InputStreamReader( fs.open(aCrushedFile)));
        String line = null;
        Map<String,String> results = new TreeMap<String,String>();
        while ((line = resultReader.readLine()) != null){
        	String [] parts= line.split("\t");
        	results.put(parts[0], parts[1]);
        }
        assertEquals(5,results.size());
        assertEquals( results.get("7"), "suzan");
        assertEquals( results.get("2"), "bob");
        resultReader.close();
    }
    /**
     * Test of compact method, of class Crush.
     *
     */
    @Test
    public void testCompact() throws Exception {
        System.out.println("compact");
        String cloneRoot = "file:/tmp/" + System.getProperty("user.name") + "/clone"+System.currentTimeMillis();
        Path clonePath = new Path(cloneRoot);

        fs.mkdirs(clonePath);

        Crush instance = new Crush();
        instance.setSourcePath(tmpRoot+"/afolder/asub1");
        instance.setClonePath(cloneRoot);
        instance.setCrushType("SEQUENCE");

        //now we need to create data in sequence files
        Path aFile = new Path(this.tmpRootPath,"afolder/asub1/file1");
        SequenceFile.Writer writer =  SequenceFile.createWriter
                (fs, jobConf, aFile, Text.class, Text.class ,
                        SequenceFile.CompressionType.BLOCK,
                new org.apache.hadoop.io.compress.GzipCodec());
        writer.append(new Text("1"), new Text("1"));
        writer.append(new Text("2"), new Text("2"));
        writer.close();
        //one more for good measure
        Path bFile = new Path(this.tmpRootPath,"afolder/asub1/file2");
        SequenceFile.Writer bwriter =  SequenceFile.createWriter
                (fs, jobConf, bFile, Text.class, Text.class ,
                        SequenceFile.CompressionType.BLOCK,
                new org.apache.hadoop.io.compress.GzipCodec());
        bwriter.append(new Text("3"), new Text("4"));
        bwriter.append(new Text("5"), new Text("6"));
        bwriter.close();

        //ready?
		instance.setConf(jobConf);
        instance.compact();
        //now we read the results for the data
        //Path aCrushedFile = new Path(this.tmpRootPath,"afolder/asub1/crushed_file");
        Path aCrushedPath = new Path(this.tmpRootPath,"afolder/asub1");
        FileStatus [] subFiles = fs.listStatus(aCrushedPath);
        assertEquals(1,  subFiles.length);
        Path aCrushedFile = subFiles[0].getPath();
        SequenceFile.Reader read1 = new SequenceFile.Reader(fs, aCrushedFile, jobConf);
        Text akey = new Text();
        Text avalue = new Text();
        HashMap<String,String> results1 = new HashMap<String,String>();
        int rowCount=0;
        while (read1.next(akey,avalue)){
            results1.put(akey.toString(), avalue.toString());
            rowCount++;
        }
        assert( results1.containsKey("1") );
        assertEquals( results1.get("1"), "1" );
        assertEquals( results1.get("2"), "2" );
        assertEquals( results1.get("5"), "6" );
        assertEquals( rowCount, 4);
        // TODO review the generated test code and remove the default call to fail.
        fs.delete(clonePath, true);
    }
    
    /**
     * Test of main method, of class Crush.
     
    @Test
    public void testMain() throws Exception {
        System.out.println("main");
        String[] args = null;
        Crush.main(args);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
     * */

    /**
     * Test of writeDiary method, of class Crush.
    */
    @Test
    public void testWriteDiary() throws Exception {
        System.out.println("writeDiary");
        String diaryFile = "/tmp/" + System.getProperty("user.name") + "/diary_"+System.currentTimeMillis();
        Path target = new Path(diaryFile);
        Crush instance = new Crush();
		instance.setConf(jobConf);
        List<String> leafs = instance.getLeafFoldersInPath( this.tmpRootPath);
        instance.writeDiary(leafs, target);
        assert(fs.exists(target));
        assert(fs.getFileStatus(target).getLen()>0);
        //a better test would be reading in the file and
        //verifying content
        fs.delete(target, true);
    }
    
    /**
     * Test of getLeafFoldersInPath method, of class Crush.
     */

    @Test
    public void testGetLeafFoldersInPath() throws Exception {
        System.out.println("getLeafFoldersInPath");
       
        Crush instance = new Crush();
		instance.setConf(jobConf);

        TreeSet<Path> expResult  = new TreeSet<Path>();
        expResult.add( new Path(this.tmpRootPath, "afolder/asub1"));
        expResult.add( new Path(this.tmpRootPath, "afolder/asub2"));
        expResult.add( new Path(this.tmpRootPath,"bfolder") );
        TreeSet<String> expResults = new TreeSet<String>();
        for (Path p: expResult){
            expResults.add(p.toUri().getPath());
        }
        List<String> result = instance.getLeafFoldersInPath( this.tmpRootPath);
        TreeSet<String> treeResult = new TreeSet();
        treeResult.addAll(result);
        assertEquals(expResults, treeResult);
        
    }
    
}