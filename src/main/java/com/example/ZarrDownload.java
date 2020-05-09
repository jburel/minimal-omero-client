package com.example;

import java.io.IOException;


import java.net.URI;
import java.net.URISyntaxException;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;


import java.io.*;
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
        String name = "6001240.zarr";
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
            File new_file = new File(parent, new_name);
            OutputStream outStream = new FileOutputStream(new_file);
            byte[] buffer = new byte[8 * 1024];
            int bytesRead;
            while ((bytesRead = responseStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, bytesRead);
            }
            responseStream.close();
            outStream.close();
        } 
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