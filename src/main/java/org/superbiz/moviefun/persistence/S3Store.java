package org.superbiz.moviefun.persistence;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.tika.Tika;
import org.apache.tika.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;

public class S3Store implements BlobStore {

    private final AmazonS3Client amazonS3Client;
    private final String bucketName;
    private final Tika tika = new Tika();

    public S3Store(AmazonS3Client s3Client, String s3BucketName) {
        this.amazonS3Client = s3Client;
        bucketName = s3BucketName;
    }

    @Override
    public void put(Blob blob) throws IOException {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        amazonS3Client.putObject(bucketName,blob.name,blob.inputStream, objectMetadata);
    }

    @Override
    public Optional<Blob> get(String name) throws IOException {
        if(!amazonS3Client.doesObjectExist(bucketName, name)) {
            return Optional.empty();
        }

        try (S3Object s3Object = amazonS3Client.getObject(bucketName, name)) {
            S3ObjectInputStream content = s3Object.getObjectContent();

            byte[] bytes = IOUtils.toByteArray(content);

            return Optional.of(new Blob(
                    name,
                    new ByteArrayInputStream(bytes),
                    tika.detect(bytes)
            ));
        }
    }

    @Override
    public void deleteAll() {
        amazonS3Client.deleteBucket(bucketName);
        amazonS3Client.createBucket(bucketName);
    }
}
