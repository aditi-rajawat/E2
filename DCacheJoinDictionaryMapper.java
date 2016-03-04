package DCacheJoin;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


public class DCacheJoinDictionaryMapper extends Mapper<LongWritable, Text, Text, Text> {
    String fileName = null, language = null;
    public Map<String, String> translations = new HashMap<String, String>();
    Path[] cachedFilePaths = null;


    public void setup(Context context) throws IOException, InterruptedException {
        // TODO: determine the name of the additional language based on the file name
        cachedFilePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        fileName = cachedFilePaths[0].getName();
        language = fileName.substring(0, fileName.lastIndexOf('.'));

        // TODO: OPTIONAL: depends on your implementation -- create a HashMap of translations (word, part of speech, translations) from output of exercise 1


    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // TODO: perform a map-side join between the word/part-of-speech from exercise 1 and the word/part-of-speech from the distributed cache file
        String partOfSpeech = null, word = null;
        String strValue = value.toString();

        partOfSpeech = strValue.substring(strValue.indexOf('['), strValue.indexOf(']')) + "]";
        word = strValue.substring(0, strValue.indexOf(':'));

        File file = new File(cachedFilePaths[0].toString());
        FileInputStream fs = null;

        try{
            fs = new FileInputStream(file);
            int c;
            while((c = fs.read())!= -1){
                String tmp = (char)c + "";
                context.write(new Text(fileName+" "+language+ ""+cachedFilePaths[0].toString()), new Text(tmp));
            }
        }
        catch (IOException e){
            System.out.println(e.getMessage());
        }
        finally {
            try{
                if(fs!=null)
                    fs.close();
            }
            catch (IOException e){
                System.out.println(e.getMessage());
            }
        }


//        FileReader fileReader = new FileReader(new File("latin.txt"));
//        BufferedReader bufferedReader = new BufferedReader(fileReader);
//        String line = bufferedReader.readLine();


        // TODO: where there is a match from above, add language:translation to the list of translations in the existing record (if no match, add language:N/A
    }

   // private String searchFile() {
//        String line = null;
//
//        try {
//            if ((cachedFilePaths != null) && (file != null)) {
//                FileReader fileReader = new FileReader(file);
//
//            }
//
//        } catch (FileNotFoundException ex) {
//            System.out.println(ex.getMessage());
//        }
//
//
//    }
}
