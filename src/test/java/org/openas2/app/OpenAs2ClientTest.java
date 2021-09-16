package org.openas2.app;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openas2.ComponentNotFoundException;
import org.openas2.TestPartner;
import org.openas2.TestResource;
import org.openas2.partner.Partnership;
import org.openas2.partner.PartnershipFactory;
import org.openas2.util.DateUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.openas2.TestUtils.waitForFile;

public class OpenAs2ClientTest {

    private static final TestResource RESOURCE = TestResource.forClass(OpenAS2ServerTest.class);

    private static TestPartner partnerA;
    private static TestPartner partnerB;

    private static OpenAS2Server serverA;
    private static OpenAS2Server serverB;

    private final int msgCnt = 1;

    private static ExecutorService executorService;
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @BeforeClass
    public static void startServers() throws Exception {
        System.setProperty("org.apache.commons.logging.Log", "org.openas2.logging.Log");
        try {
            partnerA = new TestPartner("OpenAS2A");
            partnerB = new TestPartner("OpenAS2B");

            serverA = new OpenAS2Server.Builder().run(RESOURCE.get(partnerA.getName(), "config", "config.xml").getAbsolutePath());
            partnerA.setServer(serverA);

            serverB = new OpenAS2Server.Builder().run(RESOURCE.get(partnerB.getName(), "config", "config.xml").getAbsolutePath());
            partnerB.setServer(serverB);

            //testcase Additional Config
            enhancePartners();

            executorService = Executors.newFixedThreadPool(20);

        } catch (FileNotFoundException e) {
            System.err.println("Failed to retrieve resource for test: " + e.getMessage());
            e.printStackTrace();
            throw new Exception(e);

        } catch (Throwable e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new Exception(e);
        }
    }

    private static class TestMessage{
        private final String fileName;
        private final String body;
        private final TestPartner fromPartner, toPartner;

        private TestMessage(String fileName, String body, TestPartner fromPartner, TestPartner toPartner) {
            this.fileName = fileName;
            this.body = body;
            this.fromPartner = fromPartner;
            this.toPartner = toPartner;
        }
    }

    private TestMessage sendEDI(TestPartner fromPartner, TestPartner toPartner) throws IOException {
        String EDIFileName = "sample_EDI.txt";
        String EDIMsgBody = "Sample EDI FILE BODY";
        File outEDIMsg = tmp.newFile(EDIFileName );
        FileUtils.write(outEDIMsg, EDIMsgBody, "UTF-8");

        FileUtils.copyFileToDirectory(outEDIMsg, fromPartner.getOutbox());
        return new TestMessage(EDIFileName, EDIMsgBody, fromPartner, toPartner);
    }

    @Test
    public void sendMessages() throws Exception{

        try {
            List<Callable<TestMessage>> callers = new ArrayList<Callable<TestMessage>>();
                callers.add(new Callable<TestMessage>() {
                    @Override
                    public TestMessage call() throws Exception {
                        return sendEDI(partnerA, partnerB);
                    }
                });
            for (Future<TestMessage> result : executorService.invokeAll(callers)) {
                verifyMessageDelivery(result.get());
            }
        } catch (Throwable e) {
            System.out.println("ERROR OCCURRED: " + ExceptionUtils.getStackTrace(e));
            throw new Exception(e);
        }

    }

    private void verifyMessageDelivery(TestMessage testMessage) throws IOException {

        File deliveredMsg = waitForFile(testMessage.toPartner.getInbox(), new PrefixFileFilter(testMessage.fileName), 20, TimeUnit.SECONDS);
        {
            String deliveredMsgBody = FileUtils.readFileToString(deliveredMsg, "UTF-8");
            assertThat("Verify content of delivered message", deliveredMsgBody, is(testMessage.body));
        }

        {
            File deliveryConfirmationMDN = waitForFile(testMessage.fromPartner.getRxdMDNs(), new PrefixFileFilter(testMessage.fileName), 10, TimeUnit.SECONDS);
            assertThat("Verify MDN was received by " + testMessage.fromPartner.getName(), deliveryConfirmationMDN.exists(), is(true));
        }

        {
            File deliveryConfirmationMDN = waitForFile(testMessage.toPartner.getSentMDNs(), new PrefixFileFilter(testMessage.fileName), 10, TimeUnit.SECONDS);
            assertThat("Verify MDN was stored by " + testMessage.toPartner.getName(), deliveryConfirmationMDN.exists(), is(true));
        }
    }

//    @Test
//    public void sendMessagesAsync() throws Exception{
//        int amountOfMessages = msgCnt;
//        List<Callable<TestMessage>> callers = new ArrayList<Callable<TestMessage>>(amountOfMessages);
//
//        // prepare messages
//        for (int i = 0; i < amountOfMessages; i++) {
//            callers.add(new Callable<TestMessage>() {
//                @Override
//                public TestMessage call() throws Exception {
//                    return sendEDI(partnerB, partnerA);
//                }
//            });
//
//        }
//
//        // send and verify all messages in parallel
//        for (Future<TestMessage> result : executorService.invokeAll(callers)) {
//            verifyMessageDelivery(result.get());
//        }
//    }

    private static void enhancePartners() throws ComponentNotFoundException, FileNotFoundException {
        PartnershipFactory pf = serverA.getSession().getPartnershipFactory();
        Map<String, Object> partners = pf.getPartners();
        for (Map.Entry<String, Object> pair : partners.entrySet()) {
            if (pair.getKey().equals(partnerB.getName())) {
                Map<String, String> partner = (Map<String, String>) pair.getValue();
                partnerB.setAs2Id(partner.get(Partnership.PID_AS2));
            } else if (pair.getKey().equals(partnerA.getName())) {
                Map<String, String> partner = (Map<String, String>) pair.getValue();
                partnerA.setAs2Id(partner.get(Partnership.PID_AS2));
            }
        }

        //result folder 설정
        String partnershipFolderAtoB = partnerA.getAs2Id() + "-" + partnerB.getAs2Id();
        String partnershipFolderBtoA = partnerB.getAs2Id() + "-" + partnerA.getAs2Id();

        partnerA.setHome(RESOURCE.get(partnerA.getName()));
        partnerA.setOutbox(FileUtils.getFile(partnerA.getHome(), "data", "to" + partnerB.getName()));
        partnerA.setInbox(FileUtils.getFile(partnerA.getHome(), "data", partnershipFolderBtoA, "inbox"));
        partnerA.setSentMDNs(FileUtils.getFile(partnerA.getHome(), "data", partnershipFolderBtoA, "mdn", DateUtil.formatDate("yyyy-MM-dd")));
        partnerA.setRxdMDNs(FileUtils.getFile(partnerA.getHome(), "data", partnershipFolderAtoB, "mdn", DateUtil.formatDate("yyyy-MM-dd")));

        partnerB.setHome(RESOURCE.get(partnerB.getName()));
        partnerB.setOutbox(FileUtils.getFile(partnerB.getHome(), "data", "to" + partnerA.getName()));
        partnerB.setInbox(FileUtils.getFile(partnerB.getHome(), "data", partnershipFolderAtoB, "inbox"));
        partnerB.setSentMDNs(FileUtils.getFile(partnerB.getHome(), "data", partnershipFolderAtoB, "mdn", DateUtil.formatDate("yyyy-MM-dd")));
        partnerB.setRxdMDNs(FileUtils.getFile(partnerB.getHome(), "data", partnershipFolderBtoA, "mdn", DateUtil.formatDate("yyyy-MM-dd")));

    }


    @AfterClass
    public static void tearDown() throws Exception {
        //executorService.awaitTermination(100, TimeUnit.SECONDS);
        executorService.shutdown();
        partnerA.getServer().shutdown();
        partnerB.getServer().shutdown();
    }
}
