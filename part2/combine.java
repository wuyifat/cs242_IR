package com.company;

import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.io.*;
import java.util.*;
import java.util.StringTokenizer;
import java.util.concurrent.Exchanger;

/**
 * Created by Jin on 5/15/2015.
 */
public class Combine {
    private static String prefix = "doc";
    private static String postfix = ".txt";
    private static int docid = 0;
    private static int id = 0;
    private static String pathdir = "C:\\Users\\Jin\\IdeaProjects\\searchEngine\\documents\\";
    private static String pathCombine = "C:\\Users\\Jin\\IdeaProjects\\searchEngine\\combineDoc\\";

    private static String text = "";
    private static String title = "";
    private static String everything = "";
    public static void main(String[] args) throws IOException{
        insertDocs();
    }
    public static void insertDocs() throws IOException {


        int count = 0;
        int threshold = 0;


        File dir = new File(pathdir);
        File[] directoryListing = dir.listFiles();
        for(File child : directoryListing){

            String name = child.getName();
            String docPath = child.getPath();

            try {
                Scanner x = new Scanner(new File(docPath));
                if(x.hasNext())
                    title = x.nextLine();
                while(x.hasNext())
                    text += x.nextLine();
            }catch(Exception e){}


            if(count >= threshold){
                String file = pathCombine + Integer.toString(docid) + postfix;
                File f = new File(file);
                f.createNewFile();


                try
                {
                    FileWriter fileWriter = new FileWriter(file);
                    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

                    bufferedWriter.write(name + "\t" + title + "\t" + text );
                    bufferedWriter.newLine();
                    bufferedWriter.newLine();
                    bufferedWriter.newLine();
                    // Always close files.
                    bufferedWriter.close();

                }
                catch(IOException ex) {
                    System.out.println("Error writing to file '"+ file + "'");}


                threshold += 10;
                docid++;
            }
            else if(count < threshold){
                String file = pathCombine + Integer.toString(docid) + postfix;

                try
                {
                    FileWriter fileWriter = new FileWriter(file);
                    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

                    bufferedWriter.write(name + "\t" + title + "\t" + text );
                    bufferedWriter.newLine();
                    bufferedWriter.newLine();
                    bufferedWriter.newLine();

                    // Always close files.
                    bufferedWriter.close();

                }
                catch(IOException ex) {
                    System.out.println("Error writing to file '"+ file + "'");}

            }

            count++;

        }


    }


}