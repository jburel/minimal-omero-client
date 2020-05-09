package com.example;

import java.io.IOException;


import java.net.URI;
import java.net.URISyntaxException;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;


import java.io.*;
import java.io.InputStream;
import java.util.List;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.core.ResponseInputStream;

public class ZarrDownload {

    void loadFromS3() throws Exception
    {
        String endpoint = "https://minio-dev.openmicroscopy.org/";
        //endpoint = "https://s3.embassy.ebi.ac.uk/";
        URI endpoint_uri = new URI(endpoint);
        String bucketName = "idr";
        String name = "4007801.zarr";
        String key = "zarr/v0.1/" + name + "/";
        File f = new File(name);
        f.mkdir();
        AwsCredentials credentials = AnonymousCredentialsProvider.create().resolveCredentials();
 
        S3Client client = S3Client.builder()
                .endpointOverride(endpoint_uri)
                .region(Region.US_EAST_1) // Ignored but required by the client
                .credentialsProvider(StaticCredentialsProvider.create(credentials)).build();


        List<S3Object> list = client.listObjects(ListObjectsRequest.builder()
                .bucket(bucketName)
                .prefix(key)
                .build()).contents();
        int n = list.size();
        //n = 2;
        File parent = f;
        for (int i = 0; i < n; i++) {
            S3Object object = list.get(i);
            String k = object.key();
            String new_name = k.replace(key, "");
            String[] values = new_name.split("/");
            int m = values.length;
            if (m > 1) {
                parent = createDir(f, values);
                new_name = values[m-1];
            }
            System.out.println(k);
            
            GetObjectRequest getRequest = GetObjectRequest.builder().bucket(bucketName).key(k).build();
            ResponseInputStream<GetObjectResponse> responseStream = client.getObject(getRequest, ResponseTransformer.toInputStream());
            BufferedReader reader = new BufferedReader(new InputStreamReader(responseStream));
            String line;
            File new_file = new File(parent, new_name);
            try (FileWriter writer = new FileWriter(new_file);
                 BufferedWriter bw = new BufferedWriter(writer)) {

                while ((line = reader.readLine()) != null) {            
                    bw.write(line);
                    bw.write("\n");
                }
            }
        }
        /*
        GetObjectRequest getRequest = GetObjectRequest.builder().bucket(bucketName).key(key).build();
        ResponseInputStream<GetObjectResponse> responseStream = client.getObject(getRequest, ResponseTransformer.toInputStream());
        BufferedReader reader = new BufferedReader(new InputStreamReader(responseStream));
        String line;
        try (FileWriter writer = new FileWriter("merge.py");
             BufferedWriter bw = new BufferedWriter(writer)) {

            while ((line = reader.readLine()) != null) {            
                bw.write(line);
                bw.write("\n");
            }
        }*/
        
        
    }

    private File createDir(File parent, String[] values)
    {
        int n = values.length - 1;
        for (int i = 0; i < n; i++) {
            File f = new File(parent, values[i]);
            f.mkdir();
            parent = f;
        }
        return parent;
    }

    /** Creates a new instance.*/
    ZarrDownload()
    {
    }

    /**
     */
    public static void main(String[] args) throws Exception {
        ZarrDownload client = new ZarrDownload();
        try {
            client.loadFromS3();
        } catch(Exception e) {
            e.printStackTrace();
        }   
        
    }
}