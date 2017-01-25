package com.microsoft.valda.blob;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CopyStatus;
import com.microsoft.valda.Main;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by vazvadsk on 2017-01-25.
 */
public class CopyBlobInStorage {


    public static void copy(String connectionString, String sourceContainer,
                            String targetContainer, List<String> files, int poolSize)
            throws URISyntaxException, InvalidKeyException, StorageException, InterruptedException {

        ExecutorService threadpool = Executors.newFixedThreadPool(poolSize);

        // FROM container
        CloudStorageAccount accountFrom = CloudStorageAccount.parse(connectionString);
        CloudBlobClient serviceClientFrom = accountFrom.createCloudBlobClient();
        // Container name must be lower case.
        CloudBlobContainer containerFrom = serviceClientFrom.getContainerReference(sourceContainer);
        containerFrom.createIfNotExists();
        // Container name must be lower case.
        CloudBlobContainer containerTo = serviceClientFrom.getContainerReference(targetContainer);
        containerTo.createIfNotExists();

        List<Callable<CopyBlobInStorage.AsyncBlobCopy>> tasks = new ArrayList<Callable<CopyBlobInStorage.AsyncBlobCopy>>(); // your tasks
        // invokeAll() returns when all tasks are complete

        for (String e : files) {
            tasks.add(new CopyBlobInStorage.AsyncBlobCopy(containerFrom, containerTo, e));
        }

        List<Future<CopyBlobInStorage.AsyncBlobCopy>> futures = threadpool.invokeAll(tasks);

        threadpool.shutdown();
    }

    private static class AsyncBlobCopy implements Callable {
        private String file = "";
        private CloudBlobContainer fromC = null;
        private CloudBlobContainer toC = null;

        public AsyncBlobCopy(CloudBlobContainer fromContainer, CloudBlobContainer toContainer, String fileName) {
            this.file = fileName;
            this.fromC = fromContainer;
            this.toC = toContainer;
        }

        public Long call() {
            long output = 0;
            try {
                System.out.println("Start copying for: " + file);

                CloudBlockBlob blobFrom = fromC.getBlockBlobReference(file);
                CloudBlockBlob blobTo = toC.getBlockBlobReference(file);
                blobTo.startCopy(blobFrom);
                // Exit until copy finished
                while(true) {
                    if(blobTo.getCopyState().getStatus() != CopyStatus.PENDING) {
                        break;
                    }
                    Thread.sleep(1);
                    output++;
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
    }

}
