package DCacheJoin;

import java.io.*;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class DCacheJoinDictionaryMapper extends Mapper<LongWritable, Text, Text, Text> {
    String fileName = null, language = null;
    public Map<String, String> translations = new HashMap<String, String>();
    Path[] cachedFilePaths = null;
    File file;

    public void setup(Context context) throws IOException, InterruptedException {
        // TODO: determine the name of the additional language based on the file name
        cachedFilePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        fileName = cachedFilePaths[0].getName();
        language = fileName.substring(0, fileName.lastIndexOf('.'));
        file = new File(fileName);
        // TODO: OPTIONAL: depends on your implementation -- create a HashMap of translations (word, part of speech, translations) from output of exercise 1


    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // TODO: perform a map-side join between the word/part-of-speech from exercise 1 and the word/part-of-speech from the distributed cache file
        String partOfSpeech = null, word = null;
        String strValue = value.toString();

        partOfSpeech = strValue.substring(strValue.indexOf('['), strValue.indexOf(']')) + "]";
        word = strValue.substring(0, strValue.indexOf(':'));

        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line = bufferedReader.readLine();

        context.write(new Text("test"), new Text(line));
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
