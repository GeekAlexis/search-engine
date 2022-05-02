package storage;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.regions.Regions;


// Download crawled content from RDS for transfering to S3
public class ContentDownload {

    public static void main(String[] args){
        System.out.println("start downloading");

        // Connect to RDS
        StorageFactory.connectToDatabase();
        StorageInterface storage = StorageFactory.getInstance();

        // Connect to S3
        AWSCredentials credentials = new BasicAWSCredentials(
                "AKIAZFMQMWQOSFESKUCG",
                "2anUrYUb0yWKKHaJYq6k+ioPkBOGMI3+mE7R4mhz"
        );

        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_EAST_1)
                .build();


        int startIdx = 180001;
        int endIdx = 200000;

        String bucketName = "555docbucket";
        String foldername = startIdx + "-" + endIdx;

        int batchSize = 1000;

        // Retrieve in batches of 1000
        for (int i = startIdx; i < endIdx; i += batchSize){
            System.out.println("startIdx: " + i);

            HashMap<Integer, String> map = storage.getDocumentByRange(i, batchSize);

            for (Map.Entry ele: map.entrySet()){
                int id = (int) ele.getKey();
                String content = (String) ele.getValue();

                String filename = Integer.toString(id);
                File file = new File("./rds/" + filename);

                try {
                    BufferedWriter writer = new BufferedWriter(new FileWriter("./rds/" + filename));
                    writer.write(content);

                    writer.close();
                } catch (IOException e){
                    e.printStackTrace();
                }

                s3client.putObject(
                        bucketName,
                        "in/" + filename,
                        new File("./rds/" + filename)
                );
            }
        }

    }
}