/********************************************
*Mapper
*Read text file by line. Try to parse the line by "##########". If 3 tokens are found, the first token is docID, the second
*token is title, the third token is text. If 1 token is found, it's text. Within title and text, try to parse by whitespace.
*Optimize title tokens and text tokens by discard any non-letter characters.
********************************************/
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.*;
import org.apache.hadoop.mapreduce.Mapper;
 
public class Map extends
  	Mapper<LongWritable, Text, Text, IntWritable> {
 
	private BufferedReader brReader;
	private Text txtMapOutputKey = new Text("");
	private IntWritable intMapOutputValue = new IntWritable(0);
	
	private String tokenOpt(String token) {
		token = token.toLowerCase().trim();
		newToken = "";
		for (int i = 0; i < token.length(); i++) {
			ch = token.substring(i,i+1);
			if (isLetter(ch)) 
				newToken += ch;
		}
		return newToken;
	}
	
	private boolean isLetter(String str) {
		char ch = str.charAt(0);
		if (ch >= 'a' && ch <= 'z')
			return true;
		else
			return false;
	}
 
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		int docID = 0;
		String line[] = value.toString().split("##########");		
		if (line.length == 3) {
			docID = line[0];
			String title[] = line[1].split(" ");
			String text[] = line[2].split(" ");
			for (String titleToken:title) {
				titleToken = tokenOpt(titleToken);
				txtMapOutputKey.set(docID + titleToken);
				intMapOutputValue.set(5);
				context.write(txtMapOutputKey, intMapOutputValue);
			}
			for (String textToken:text) {
				textToken = tokenOpt(textToken);
				txtMapOutputKey.set(docID + textToken);
				intMapOutputValue.set(1);
				context.write(txtMapOutputKey, intMapOutputValue);
			}
		else if (line.length == 1) {
			String text[] = line[0].split(" ");
			for (String textToken:text) {
				textToken = tokenOpt(textToken);
				txtMapOutputKey.set(docID + textToken);
				intMapOutputValue.set(1);
				context.write(txtMapOutputKey, intMapOutputValue);
			}
		}
		else {
			txtMapOutputKey.set("wuyifat");
			intMapOutputValue.set(1000);
		}

	}
}