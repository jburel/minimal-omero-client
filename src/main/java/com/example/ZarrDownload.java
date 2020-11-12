package com.example;

import java.io.IOException;


import java.net.URI;
import java.net.URISyntaxException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import software.amazon.awssdk.core.client.config.*;
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
import software.amazon.awssdk.services.s3.S3Configuration;

import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.core.ResponseInputStream;

import org.apache.commons.io.IOUtils;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import javax.swing.*;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;

public class ZarrDownload {

    void loadFromS3() throws Exception
    {
        String endpoint = "https://minio-dev.openmicroscopy.org/";
        endpoint = "https://s3.embassy.ebi.ac.uk/";
        URI endpoint_uri = new URI(endpoint);
        String bucketName = "idr";
        String nfile = "6001247.zarr";
        if (endpoint.contains("minio-dev")) {
            nfile = "idr/6001240.zarr";
        }
        String name = "idr/"+nfile;
        String key = "zarr/v0.1/" + nfile ;
        Path f = Files.createDirectories(Paths.get("/tmp/"+name));
        System.err.println("directory");
        final S3Configuration config = S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .build();
        //SdkClientConfiguration clientConfig = SdkClientConfiguration.builder()
        //        .option(SdkClientOption.ENDPOINT, endpoint_uri).build();
        AwsCredentials credentials = AnonymousCredentialsProvider.create().resolveCredentials();
        S3Client client = S3Client.builder()
                .endpointOverride(endpoint_uri)
                .serviceConfiguration(config)
                .region(Region.EU_WEST_1) // Ignored but required by the client
                .credentialsProvider(StaticCredentialsProvider.create(credentials)).build();
        System.err.println(key);


        List<S3Object> list = client.listObjects(ListObjectsRequest.builder()
                .bucket(bucketName)
                .prefix(key)
                .build()).contents();
        int n = list.size();
        //n = 2;
        System.err.println(n);
        File parent = f.toFile();
        for (int i = 0; i < n; i++) {
            S3Object object = list.get(i);
            String k = object.key();
            String new_name = k.replace(key, "");
            String[] values = new_name.split("/");
            int m = values.length;
            if (m > 1) {
                parent = createDir(f.toFile(), values);
                new_name = values[m-1];
            }

            GetObjectRequest getRequest = GetObjectRequest.builder().bucket(bucketName).key(k).build();
            ResponseInputStream<GetObjectResponse> responseStream = client.getObject(getRequest, ResponseTransformer.toInputStream());
            System.out.println(new_name);
            File new_file = new File(parent, new_name);
            new_file.createNewFile();

            OutputStream outputStream = new FileOutputStream(new_file);
            IOUtils.copy(responseStream, outputStream);
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