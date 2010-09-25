/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package crush;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.util.HashMap;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import static org.junit.Assert.*;

/**
 *
 * @author edward
 */
public class CrushUtilTest {

    private JobConf jobConf;
    private FileSystem fs;
    private String tmpRoot;
    private Path tmpRootPath;

    private String cloneRoot;
    private Path cloneRootPath;

    public CrushUtilTest() {
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
    public void testCrushFile() throws Exception {
        Path aFile = new Path(this.tmpRootPath, "filed");
        FSDataOutputStream out = fs.create(aFile);
        BufferedOutputStream bw = new java.io.BufferedOutputStream(out);
        byte[] data1 = new byte[]{'a', 'b', '\n'};
        byte[] data2 = new byte[]{'c', '4', '\n'};
        bw.write(data1);
        bw.write(data2);
        bw.close();

        Path bFile = new Path(this.tmpRootPath, "filec");
        FSDataOutputStream outb = fs.create(bFile);
        BufferedOutputStream bw2 = new java.io.BufferedOutputStream(outb);
        byte[] data3 = new byte[]{'e', 'p', '\n'};
        byte[] data4 = new byte[]{'9', 'k', '\n'};
        bw2.write(data3);
        bw2.write(data4);
        bw2.close();

        CrushUtil instance = new CrushUtil();
        instance.setSourcePath(tmpRootPath);
        instance.setOutPath(new Path(tmpRootPath,"crushed_file_text"));
        instance.setType(CrushUtil.FileType.TEXT);

        instance.crush();

        BufferedInputStream br = new BufferedInputStream(fs.open(new Path(tmpRootPath, "crushed_file_text")));
        byte[] buffer = new byte[2048];
        int read = -1;
        int lineCount = 0;
        while ((read = br.read(buffer)) != -1) {
            bw.write(buffer, 0, read);
            lineCount++;

        }

        br.close();


    }
    /**
     * Test of crush method, of class CrushUtil.
     */
    @Test
    public void testCrush() throws Exception {

        Path aFile = new Path(this.tmpRootPath,"filea");
        SequenceFile.Writer writer =  SequenceFile.createWriter
                (fs, jobConf, aFile, Text.class, Text.class ,
                        SequenceFile.CompressionType.BLOCK,
                new org.apache.hadoop.io.compress.GzipCodec());
        writer.append(new Text("1"), new Text("1"));
        writer.append(new Text("2"), new Text("2"));
        writer.close();

        Path bFile = new Path(this.tmpRootPath,"fileb");
        SequenceFile.Writer writerb =  SequenceFile.createWriter
                (fs, jobConf, bFile, Text.class, Text.class ,
                        SequenceFile.CompressionType.BLOCK,
                new org.apache.hadoop.io.compress.GzipCodec());
        writerb.append(new Text("3"), new Text("4"));
        writerb.append(new Text("5"), new Text("6"));
        writerb.close();

        CrushUtil instance = new CrushUtil();
        instance.setSourcePath(tmpRootPath);
        instance.setOutPath(new Path(tmpRootPath,"crushed_file"));        
        instance.setType(CrushUtil.FileType.SEQUENCEFILE);
        instance.crush();

        SequenceFile.Reader read1 = new SequenceFile.Reader(fs, new Path(tmpRootPath,"crushed_file"), jobConf);
        assert( fs.exists( new Path(tmpRootPath,"crushed_file") ) );
        Text akey = new Text();
        Text avalue = new Text();
        HashMap<String,String> results1 = new HashMap<String,String>();
        int rowCount=0;
        while (read1.next(akey,avalue)){
            results1.put(akey.toString(), avalue.toString());
            rowCount++;
        }

        assertEquals( 4, rowCount);
        assert( results1.containsKey("1") );
        assertEquals( results1.get("1"), "1" );
        assertEquals( results1.get("2"), "2" );
        assertEquals( results1.get("5"), "6" );
        

       
    }

    

}