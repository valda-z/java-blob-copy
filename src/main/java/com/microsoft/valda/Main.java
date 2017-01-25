package com.microsoft.valda;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.valda.blob.CopyBlobBetweenStorages;
import com.microsoft.valda.blob.CopyBlobInStorage;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by vazvadsk on 2017-01-24.
 */
public class Main {

    public static void main(String [] args) throws InterruptedException, ExecutionException, URISyntaxException, InvalidKeyException, StorageException {

        String filesToCopy =
            "DSCN4678.JPG,"+
            "DSCN4679.JPG,"+
            "DSCN4682.JPG,"+
            "DSCN4685.JPG,"+
            "DSCN4688.JPG,"+
            "DSCN4690.JPG,"+
            "DSCN4693.JPG,"+
            "DSCN4695.JPG,"+
            "DSCN4697.JPG,"+
            "DSCN4699.JPG,"+
            "DSCN4700.JPG,"+
            "DSCN4705.JPG,"+
            "DSCN4714.JPG,"+
            "DSCN4716.JPG,"+
            "DSCN4717.JPG,"+
            "DSCN4718.JPG,"+
            "DSCN4719.JPG,"+
            "DSCN4720.JPG,"+
            "DSCN4721.MOV,"+
            "DSCN4725.JPG,"+
            "DSCN4727.JPG,"+
            "DSCN4729.JPG,"+
            "DSCN4732.JPG,"+
            "DSCN4734.JPG,"+
            "DSCN4736.JPG,"+
            "DSCN4742.JPG,"+
            "DSCN4744.JPG,"+
            "DSCN4746.JPG,"+
            "DSCN4748.JPG,"+
            "DSCN4752.JPG,"+
            "DSCN4754.JPG,"+
            "DSCN4760.JPG";

        List<String> files = Arrays.asList(filesToCopy.split(","));


            CopyBlobInStorage.copy(
                    "DefaultEndpointsProtocol=http;"
                            + "AccountName=<STORAGE ACCOUNT NAME>;"
                            + "AccountKey=<PRIMARY KEY>",
                    "source-container-name",
                    "target-container-name",
                    files,
                    16
            );

            CopyBlobBetweenStorages.copy(
                    "DefaultEndpointsProtocol=http;"
                            + "AccountName=<STORAGE ACCOUNT NAME>;"
                            + "AccountKey=<PRIMARY KEY>",
                    "DefaultEndpointsProtocol=http;"
                            + "AccountName=<STORAGE ACCOUNT NAME>;"
                            + "AccountKey=<PRIMARY KEY>",
                    "source-container-name",
                    "target-container-name",
                    files,
                    16
            );
    }

}

