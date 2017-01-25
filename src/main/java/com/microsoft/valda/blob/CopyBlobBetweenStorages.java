package com.microsoft.valda.blob;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by vazvadsk on 2017-01-25.
 */
public class CopyBlobBetweenStorages {

    public static void copy(String sourceConnectionString, String targetConnectionString, String sourceContainer,
                            String targetContainer, List<String> files, int poolSize)
            throws URISyntaxException, InvalidKeyException, StorageException, InterruptedException {
        ExecutorService threadpool = Executors.newFixedThreadPool(poolSize);

        // FROM container
        CloudStorageAccount accountFrom = CloudStorageAccount.parse(sourceConnectionString);
        CloudBlobClient serviceClientFrom = accountFrom.createCloudBlobClient();
        // Container name must be lower case.
        CloudBlobContainer containerFrom = serviceClientFrom.getContainerReference(sourceContainer);
        containerFrom.createIfNotExists();

        CloudStorageAccount accountTo = CloudStorageAccount.parse(targetConnectionString);
        CloudBlobClient serviceClientTo = accountTo.createCloudBlobClient();
        // Container name must be lower case.
        CloudBlobContainer containerTo = serviceClientTo.getContainerReference(targetContainer);
        containerTo.createIfNotExists();

        List<Callable<CopyBlobBetweenStorages.AsyncBlobCopy>> tasks = new ArrayList<Callable<CopyBlobBetweenStorages.AsyncBlobCopy>>(); // your tasks
        // invokeAll() returns when all tasks are complete

        // generate sas token
        String sas = generateSAS(containerFrom);

        for (String e : files) {
            tasks.add(new CopyBlobBetweenStorages.AsyncBlobCopy(containerFrom, containerTo, sas, e));
        }

        List<Future<CopyBlobBetweenStorages.AsyncBlobCopy>> futures = threadpool.invokeAll(tasks);

        threadpool.shutdown();

    }

    private static String generateSAS(CloudBlobContainer container) throws StorageException, InvalidKeyException {
        System.out.println("Generating SAS for source containe...");
        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
        GregorianCalendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        calendar.setTime(new Date());
        policy.setSharedAccessStartTime(calendar.getTime());
        // LETS generate policy for 4 hours
        calendar.add(Calendar.HOUR, 4);
        policy.setSharedAccessExpiryTime(calendar.getTime());
        policy.setPermissions(EnumSet.of(SharedAccessBlobPermissions.READ));
        String sas = container.generateSharedAccessSignature(policy, null);
        System.out.println("Generated SAS for source container: " + sas);
        return sas;
    }

    private static class AsyncBlobCopy implements Callable {
        private String file = "";
        private CloudBlobContainer fromC = null;
        private CloudBlobContainer toC = null;
        private String sasToken = "";

        public AsyncBlobCopy(CloudBlobContainer fromContainer, CloudBlobContainer toContainer, String sas, String fileName) {
            this.file = fileName;
            this.fromC = fromContainer;
            this.toC = toContainer;
            this.sasToken = sas;
        }

        public Long call() {
            long output = 0;
            try {
                System.out.println("Start copying for: " + file);

                CloudBlockBlob blobFrom = fromC.getBlockBlobReference(file);
                CloudBlockBlob blobTo = toC.getBlockBlobReference(file);
                blobTo.startCopy( new URI(blobFrom.getUri() + "?" + sasToken));
                blobTo.downloadAttributes();
                // Exit until copy finished
                while(true) {
                    if(blobTo.getCopyState().getStatus() != CopyStatus.PENDING) {
                        break;
                    }
                    Thread.sleep(25);
                    output+=25;
                    blobTo.downloadAttributes();
                }

            } catch (InterruptedException ex) {
                ex.printStackTrace();
                output = -1;
            } catch (StorageException e) {
                e.printStackTrace();
                output = -2;
            } catch (URISyntaxException e) {
                e.printStackTrace();
                output = -3;
            }

            System.out.println("File copied: " + file + " ... copy time: " + output);
            return output;
        }
    }}
