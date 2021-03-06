/*
 *  Copyright (c) Microsoft. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.sdk.iot.common.tests.iothubservices;

import com.microsoft.azure.sdk.iot.common.helpers.*;
import com.microsoft.azure.sdk.iot.common.helpers.Tools;
import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.service.*;
import com.microsoft.azure.sdk.iot.service.auth.AuthenticationType;
import com.microsoft.azure.sdk.iot.service.exceptions.IotHubException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.microsoft.azure.sdk.iot.common.helpers.CorrelationDetailsLoggingAssert.buildExceptionMessage;
import static com.microsoft.azure.sdk.iot.common.tests.iothubservices.FileUploadTests.STATUS.FAILURE;
import static com.microsoft.azure.sdk.iot.common.tests.iothubservices.FileUploadTests.STATUS.SUCCESS;
import static com.microsoft.azure.sdk.iot.device.IotHubStatusCode.OK;
import static com.microsoft.azure.sdk.iot.device.IotHubStatusCode.OK_EMPTY;
import static org.junit.Assert.*;

/**
 * Test class containing all tests to be run on JVM and android pertaining to FileUpload. Class needs to be extended
 * in order to run these tests as that extended class handles setting connection strings and certificate generation
 */
public class FileUploadTests extends IntegrationTest
{
    // Max time to wait to see it on Hub
    private static final long MAXIMUM_TIME_TO_WAIT_FOR_IOTHUB = 180000; // 3 minutes
    private static final long FILE_UPLOAD_QUEUE_POLLING_INTERVAL = 4000; // 4 sec
    private static final long MAXIMUM_TIME_TO_WAIT_FOR_CALLBACK = 5000; // 5 sec

    //Max time to wait before timing out test
    private static final long MAX_MILLISECS_TIMEOUT_KILL_TEST = MAXIMUM_TIME_TO_WAIT_FOR_IOTHUB + 50000; // 50 secs

    //Max devices to test
    private static final Integer MAX_FILES_TO_UPLOAD = 5;

    // remote name of the file
    private static final String REMOTE_FILE_NAME = "File";
    private static final String REMOTE_FILE_NAME_EXT = ".txt";

    protected static String iotHubConnectionString = "";

    // States of SDK
    private static RegistryManager registryManager;
    private static ServiceClient serviceClient;
    private static FileUploadNotificationReceiver fileUploadNotificationReceiver;

    private static String publicKeyCertificate;
    private static String privateKeyCertificate;
    private static String x509Thumbprint;

    static Set<FileUploadNotification> activeFileUploadNotifications = new ConcurrentSkipListSet<>(new Comparator<FileUploadNotification>()
    {
        @Override
        public int compare(FileUploadNotification o1, FileUploadNotification o2) {
            if (!o1.getDeviceId().equals(o2.getDeviceId()))
            {
                return -1;
            }

            if (!o1.getBlobName().equals(o2.getBlobName()))
            {
                return -1;
            }

            if (!o1.getBlobSizeInBytes().equals(o2.getBlobSizeInBytes()))
            {
                return -1;
            }

            if (!o1.getBlobUri().equals(o2.getBlobUri()))
            {
                return -1;
            }

            if (!o1.getEnqueuedTimeUtcDate().equals(o2.getEnqueuedTimeUtcDate()))
            {
                return -1;
            }

            if (!o1.getLastUpdatedTimeDate().equals(o2.getLastUpdatedTimeDate()))
            {
                return -1;
            }

            return 0;
        }
    });
    static Thread fileUploadNotificationListenerThread;
    static AtomicBoolean hasFileUploadNotificationReceiverThreadFailed = new AtomicBoolean(false);
    static Exception fileUploadNotificationReceiverThreadException = null;

    public static Collection inputs() throws Exception
    {
        X509CertificateGenerator certificateGenerator = new X509CertificateGenerator();
        return inputs(certificateGenerator.getPublicCertificate(), certificateGenerator.getPrivateKey(), certificateGenerator.getX509Thumbprint());
    }

    public static Collection inputs(String publicK, String privateK, String thumbprint) throws Exception
    {
        registryManager = RegistryManager.createFromConnectionString(iotHubConnectionString);

        serviceClient = ServiceClient.createFromConnectionString(iotHubConnectionString, IotHubServiceClientProtocol.AMQPS);
        serviceClient.open();

        publicKeyCertificate = publicK;
        privateKeyCertificate = privateK;
        x509Thumbprint = thumbprint;

        fileUploadNotificationListenerThread = createFileUploadNotificationListenerThread();

        return Arrays.asList(
                new Object[][]
                        {
                                {IotHubClientProtocol.HTTPS, AuthenticationType.SAS},
                                {IotHubClientProtocol.HTTPS, AuthenticationType.SELF_SIGNED}
                        });
    }

    public FileUploadTests(IotHubClientProtocol protocol, AuthenticationType authenticationType) throws InterruptedException, IOException, IotHubException, URISyntaxException
    {
        this.testInstance = new FileUploadTestInstance(protocol, authenticationType);
    }

    public FileUploadTestInstance testInstance;

    public class FileUploadTestInstance
    {
        public IotHubClientProtocol protocol;
        public AuthenticationType authenticationType;
        private FileUploadState[] fileUploadState;
        private MessageState[] messageStates;

        public FileUploadTestInstance(IotHubClientProtocol protocol, AuthenticationType authenticationType) throws InterruptedException, IOException, IotHubException, URISyntaxException
        {
            this.protocol = protocol;
            this.authenticationType = authenticationType;
        }
    }

    enum STATUS
    {
        SUCCESS, FAILURE
    }

    private static class FileUploadState
    {
        String blobName;
        InputStream fileInputStream;
        long fileLength;
        boolean isCallBackTriggered;
        STATUS fileUploadStatus;
        STATUS fileUploadNotificationReceived;
    }

    private static class MessageState
    {
        String messageBody;
        STATUS messageStatus;
    }

    private static class FileUploadCallback implements IotHubEventCallback
    {
        @Override
        public void execute(IotHubStatusCode responseStatus, Object context)
        {
            FileUploadState f = (FileUploadState) context;
            System.out.println("Callback fired with status " + responseStatus);
            if (context instanceof FileUploadState)
            {
                FileUploadState fileUploadState = (FileUploadState) context;
                fileUploadState.isCallBackTriggered = true;

                // On failure, Don't update fileUploadStatus any further
                if ((responseStatus == OK || responseStatus == OK_EMPTY) && fileUploadState.fileUploadStatus != FAILURE)
                {
                    fileUploadState.fileUploadStatus = SUCCESS;
                }
                else
                {
                    fileUploadState.fileUploadStatus = FAILURE;
                }
            }
            else if (context instanceof MessageState)
            {
                MessageState messageState = (MessageState) context;
                // On failure, Don't update message status any further
                if ((responseStatus == OK || responseStatus == OK_EMPTY) && messageState.messageStatus != FAILURE)
                {
                    messageState.messageStatus = SUCCESS;
                }
                else
                {
                    messageState.messageStatus = FAILURE;
                }
            }
        }
    }

    private static class FileUploadNotificationListener implements Runnable
    {
        @Override
        public void run()
        {
            try
            {
                // flush pending notifications before every test to prevent random test failures
                // because of notifications received from other failed test
                fileUploadNotificationReceiver = serviceClient.getFileUploadNotificationReceiver();

                // Start receiver for a test
                fileUploadNotificationReceiver.open();

                while (true)
                {
                    FileUploadNotification notification = fileUploadNotificationReceiver.receive(FILE_UPLOAD_QUEUE_POLLING_INTERVAL);
                    if (notification != null)
                    {
                        System.out.println("Received notification for device " + notification.getDeviceId());
                        activeFileUploadNotifications.add(notification);
                    }
                }
            }
            catch (IOException e)
            {
                fileUploadNotificationReceiverThreadException = e;
                hasFileUploadNotificationReceiverThreadFailed.set(true);
            }
            catch (InterruptedException e)
            {
                try
                {
                    fileUploadNotificationReceiver.close();
                }
                catch (IOException e1)
                {
                    fileUploadNotificationReceiverThreadException = e1;
                    hasFileUploadNotificationReceiverThreadFailed.set(true);
                }
            }
        }
    }

    /**
     * Spawn a thread to constantly listen for file upload notifications. When one is found, it adds it to the active set of
     * notifications for the tests in this class to consume.
     */
    private static Thread createFileUploadNotificationListenerThread()
    {
        FileUploadNotificationListener fileUploadNotificationListener = new FileUploadNotificationListener();
        fileUploadNotificationListenerThread = new Thread(fileUploadNotificationListener);
        fileUploadNotificationListenerThread.start();
        return fileUploadNotificationListenerThread;
    }

    @Before
    public void setUpFileUploadState() throws Exception
    {
        testInstance.fileUploadState = new FileUploadState[MAX_FILES_TO_UPLOAD];
        testInstance.messageStates = new MessageState[MAX_FILES_TO_UPLOAD];
        for (int i = 0; i < MAX_FILES_TO_UPLOAD; i++)
        {
            byte[] buf = new byte[i];
            new Random().nextBytes(buf);
            testInstance.fileUploadState[i] = new FileUploadState();
            testInstance.fileUploadState[i].blobName = REMOTE_FILE_NAME + i + REMOTE_FILE_NAME_EXT;
            testInstance.fileUploadState[i].fileInputStream = new ByteArrayInputStream(buf);
            testInstance.fileUploadState[i].fileLength = buf.length;
            testInstance.fileUploadState[i].fileUploadStatus = SUCCESS;
            testInstance.fileUploadState[i].fileUploadNotificationReceived = FAILURE;
            testInstance.fileUploadState[i].isCallBackTriggered = false;

            testInstance.messageStates[i] = new MessageState();
            testInstance.messageStates[i].messageBody = new String(buf);
            testInstance.messageStates[i].messageStatus = SUCCESS;
        }
    }

    @AfterClass
    public static void tearDown() throws IotHubException, IOException, InterruptedException
    {
        if (registryManager != null)
        {
            registryManager.close();
            registryManager = null;
        }

        serviceClient = null;

        fileUploadNotificationListenerThread.interrupt();
        fileUploadNotificationListenerThread.stop();
    }

    private DeviceClient setUpDeviceClient(IotHubClientProtocol protocol) throws URISyntaxException, InterruptedException, IOException, IotHubException
    {
        DeviceClient deviceClient;
        if (testInstance.authenticationType == AuthenticationType.SAS)
        {
            String deviceId = "java-file-upload-e2e-test-".concat(UUID.randomUUID().toString());
            Device scDevice = com.microsoft.azure.sdk.iot.service.Device.createFromId(deviceId, null, null);
            scDevice = Tools.addDeviceWithRetry(registryManager, scDevice);

            deviceClient = new DeviceClient(DeviceConnectionString.get(iotHubConnectionString, scDevice), protocol);

        }
        else if (testInstance.authenticationType == AuthenticationType.SELF_SIGNED)
        {
            String deviceIdX509 = "java-file-upload-e2e-test-x509-".concat(UUID.randomUUID().toString());
            Device scDevicex509 = com.microsoft.azure.sdk.iot.service.Device.createDevice(deviceIdX509, AuthenticationType.SELF_SIGNED);
            scDevicex509.setThumbprintFinal(x509Thumbprint, x509Thumbprint);
            scDevicex509 = Tools.addDeviceWithRetry(registryManager, scDevicex509);

            deviceClient = new DeviceClient(DeviceConnectionString.get(iotHubConnectionString, scDevicex509), protocol, publicKeyCertificate, false, privateKeyCertificate, false);
        }
        else
        {
            throw new IllegalArgumentException("Test code has not been written for this authentication type yet");
        }

        Thread.sleep(5000);

        IotHubServicesCommon.openClientWithRetry(deviceClient);
        return deviceClient;
    }

    private void tearDownDeviceClient(DeviceClient deviceClient) throws IOException
    {
        deviceClient.closeNow();
    }

    private void verifyNotification(FileUploadNotification fileUploadNotification, FileUploadState fileUploadState, DeviceClient deviceClient) throws IOException
    {
        assertTrue(buildExceptionMessage("File upload notification blob size not equal to expected file length", deviceClient), fileUploadNotification.getBlobSizeInBytes() == fileUploadState.fileLength);

        URL u = new URL(fileUploadNotification.getBlobUri());
        try (InputStream inputStream = u.openStream())
        {
            byte[] testBuf = new byte[(int)fileUploadState.fileLength];
            int testLen = inputStream.read(testBuf,  0, (int)fileUploadState.fileLength);
            byte[] actualBuf = new byte[(int)fileUploadState.fileLength];
            fileUploadState.fileInputStream.reset();
            int actualLen = (fileUploadState.fileLength == 0) ? (int) fileUploadState.fileLength : fileUploadState.fileInputStream.read(actualBuf, 0, (int) fileUploadState.fileLength);
            assertEquals(buildExceptionMessage("Expected length " + testLen + " but was " + actualLen, deviceClient), testLen, actualLen);
            assertTrue(buildExceptionMessage("testBuf was different from actualBuf", deviceClient), Arrays.equals(testBuf, actualBuf));
        }

        assertTrue(buildExceptionMessage("File upload notification did not contain the expected blob name", deviceClient), fileUploadNotification.getBlobName().contains(fileUploadState.blobName));
        fileUploadState.fileUploadNotificationReceived = SUCCESS;
    }

    @Test (timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void uploadToBlobAsyncSingleFileZeroLength() throws URISyntaxException, IOException, InterruptedException, IotHubException
    {
        // arrange
        DeviceClient deviceClient = setUpDeviceClient(testInstance.protocol);

        // act
        deviceClient.uploadToBlobAsync(testInstance.fileUploadState[0].blobName, testInstance.fileUploadState[0].fileInputStream, testInstance.fileUploadState[0].fileLength, new FileUploadCallback(), testInstance.fileUploadState[0]);

        // assert
        if (!isBasicTierHub)
        {
            FileUploadNotification fileUploadNotification = getFileUploadNotificationForThisDevice(deviceClient, 0);
            verifyNotification(fileUploadNotification, testInstance.fileUploadState[0], deviceClient);
        }
        waitForFileUploadStatusCallbackTriggered(0, deviceClient);
        assertEquals(buildExceptionMessage("File upload status expected SUCCESS but was " + testInstance.fileUploadState[0].fileUploadStatus, deviceClient), SUCCESS, testInstance.fileUploadState[0].fileUploadStatus);
        tearDownDeviceClient(deviceClient);
    }

    @Test (timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void uploadToBlobAsyncSingleFile() throws URISyntaxException, IOException, InterruptedException, IotHubException
    {
        // arrange
        DeviceClient deviceClient = setUpDeviceClient(testInstance.protocol);

        // act
        deviceClient.uploadToBlobAsync(testInstance.fileUploadState[MAX_FILES_TO_UPLOAD - 1].blobName, testInstance.fileUploadState[MAX_FILES_TO_UPLOAD - 1].fileInputStream, testInstance.fileUploadState[MAX_FILES_TO_UPLOAD - 1].fileLength, new FileUploadCallback(), testInstance.fileUploadState[MAX_FILES_TO_UPLOAD - 1]);

        // assert
        if (!isBasicTierHub)
        {
            FileUploadNotification fileUploadNotification = getFileUploadNotificationForThisDevice(deviceClient, MAX_FILES_TO_UPLOAD - 1);
            assertNotNull(buildExceptionMessage("file upload notification was null", deviceClient), fileUploadNotification);
            verifyNotification(fileUploadNotification, testInstance.fileUploadState[MAX_FILES_TO_UPLOAD - 1], deviceClient);
        }
        waitForFileUploadStatusCallbackTriggered(MAX_FILES_TO_UPLOAD - 1, deviceClient);
        assertEquals(buildExceptionMessage("File upload status should be SUCCESS but was " + testInstance.fileUploadState[MAX_FILES_TO_UPLOAD - 1].fileUploadStatus, deviceClient), SUCCESS, testInstance.fileUploadState[MAX_FILES_TO_UPLOAD - 1].fileUploadStatus);

        tearDownDeviceClient(deviceClient);
    }

    @Test (timeout = MAX_MILLISECS_TIMEOUT_KILL_TEST)
    public void uploadToBlobAsyncMultipleFilesParallel() throws URISyntaxException, IOException, InterruptedException, ExecutionException, TimeoutException, IotHubException
    {
        // arrange
        DeviceClient deviceClient = setUpDeviceClient(testInstance.protocol);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        // act
        for (int i = 1; i < MAX_FILES_TO_UPLOAD; i++)
        {
            final int index = i;
            executor.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        deviceClient.uploadToBlobAsync(testInstance.fileUploadState[index].blobName, testInstance.fileUploadState[index].fileInputStream, testInstance.fileUploadState[index].fileLength, new FileUploadCallback(), testInstance.fileUploadState[index]);
                    }
                    catch (IOException e)
                    {
                        fail(buildExceptionMessage("IOException occurred during upload: " + e.getMessage(), deviceClient));
                    }
                }
            });

            // assert
            if (!isBasicTierHub)
            {
                FileUploadNotification fileUploadNotification = getFileUploadNotificationForThisDevice(deviceClient, i);
                verifyNotification(fileUploadNotification, testInstance.fileUploadState[i], deviceClient);
            }
            waitForFileUploadStatusCallbackTriggered(i, deviceClient);
            assertEquals(buildExceptionMessage("Expected SUCCESS but file upload status " + i + " was " + testInstance.fileUploadState[i].fileUploadStatus, deviceClient), SUCCESS, testInstance.fileUploadState[i].fileUploadStatus);
            assertEquals(buildExceptionMessage("Expected SUCCESS but message status " + i + " was " + testInstance.messageStates[i].messageStatus, deviceClient), SUCCESS, testInstance.messageStates[i].messageStatus);
        }

        executor.shutdown();
        if (!executor.awaitTermination(10000, TimeUnit.MILLISECONDS))
        {
            executor.shutdownNow();
        }

        if (!isBasicTierHub)
        {
            for (int i = 1; i < MAX_FILES_TO_UPLOAD; i++)
            {
                assertEquals(buildExceptionMessage("File" + i + " has no notification", deviceClient), testInstance.fileUploadState[i].fileUploadNotificationReceived, SUCCESS);
            }
        }

        tearDownDeviceClient(deviceClient);
    }

    private FileUploadNotification getFileUploadNotificationForThisDevice(DeviceClient deviceClient, int expectedBlobSizeInBytes) throws IOException, InterruptedException
    {
        //wait until the notification is added to the set of retrieved notifications, or until a timeout
        long startTime = System.currentTimeMillis();
        FileUploadNotification matchingNotification = null;
        do
        {
            for (FileUploadNotification notification : activeFileUploadNotifications)
            {
                if (notification.getDeviceId().equals(deviceClient.getConfig().getDeviceId()))
                {
                    if (notification.getBlobSizeInBytes().intValue() == expectedBlobSizeInBytes)
                    {
                        matchingNotification = notification;
                    }
                }
            }

            if (System.currentTimeMillis() - startTime > MAXIMUM_TIME_TO_WAIT_FOR_IOTHUB)
            {
                Assert.fail(CorrelationDetailsLoggingAssert.buildExceptionMessage("Timed out waiting for file upload notification for device", deviceClient));
            }

            //If the notification polling thread has died, the test cannot complete
            if (hasFileUploadNotificationReceiverThreadFailed.get())
            {
                if (fileUploadNotificationReceiverThreadException != null)
                {
                    Assert.fail(CorrelationDetailsLoggingAssert.buildExceptionMessage("File upload notification listener thread has died from exception " + Tools.getStackTraceFromThrowable(fileUploadNotificationReceiverThreadException), deviceClient));
                }
                else
                {
                    Assert.fail(CorrelationDetailsLoggingAssert.buildExceptionMessage("File upload notification listener thread has died from an unknown exception", deviceClient));
                }
            }

            Thread.sleep(2000);

        } while (matchingNotification == null);

        if (matchingNotification != null)
        {
            activeFileUploadNotifications.remove(matchingNotification);
        }

        return matchingNotification;
    }

    private void waitForFileUploadStatusCallbackTriggered(int fileUploadStateIndex, DeviceClient deviceClient) throws InterruptedException
    {
        if (!testInstance.fileUploadState[fileUploadStateIndex].isCallBackTriggered)
        {
            //wait until file upload callback is triggered
            long startTime = System.currentTimeMillis();
            while (!testInstance.fileUploadState[fileUploadStateIndex].isCallBackTriggered)
            {
                Thread.sleep(300);
                if (System.currentTimeMillis() - startTime > MAXIMUM_TIME_TO_WAIT_FOR_CALLBACK)
                {
                    assertTrue(buildExceptionMessage("File upload callback was not triggered", deviceClient), testInstance.fileUploadState[fileUploadStateIndex].isCallBackTriggered);
                }
            }
        }
    }
}
