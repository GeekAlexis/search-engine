package storage;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

// Download crawled content from RDS for transfering to S3
public class ContentDownload {

    public static void main(String[] args){
        System.out.println("start downloading");

        StorageFactory.connectToDatabase();
        StorageInterface storage = StorageFactory.getInstance();

        int start = 120001;
        int numDoc = 1000;

        // In batch of 1,000
        for (int i = start; i < start + 10000; i += numDoc){
            int startIdx = i;
            int endIdx = startIdx + numDoc - 1;

            System.out.println("starting index: " + startIdx);
            System.out.println("ending index: " + endIdx);

            String rootpath = "./rds";
            String foldername = startIdx + "-" + endIdx;
            String folderpath = rootpath + "/" + foldername;
            System.out.println("folderpath: " + folderpath);

            File directory = new File(folderpath);

            if (! directory.exists()){
                directory.mkdirs();
            }

            HashMap<Integer, String> map = storage.getDocumentByRange(startIdx, numDoc);

            for (Map.Entry ele: map.entrySet()){
                int id = (int) ele.getKey();
                String content = (String) ele.getValue();

                String filename = folderpath + "/" + id;
                File file = new File(filename);

                try {
                    BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
                    writer.write(content);

                    writer.close();
                } catch (IOException e){
                    e.printStackTrace();
                }
            }
        }
    }
}