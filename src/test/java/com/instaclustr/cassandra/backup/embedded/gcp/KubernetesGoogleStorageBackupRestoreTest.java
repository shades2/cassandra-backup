package com.instaclustr.cassandra.backup.embedded.gcp;

import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.cassandra.backup.gcp.GCPModule;
import com.instaclustr.cassandra.backup.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.kubernetes.KubernetesApiModule;
import com.instaclustr.kubernetes.KubernetesService;
import com.instaclustr.threading.ExecutorsModule;
import io.kubernetes.client.ApiException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Test(groups = {
    "k8sTest",
    "googleTest",
})
public class KubernetesGoogleStorageBackupRestoreTest extends BaseGoogleStorageBackupRestoreTest {

    @Inject
    public KubernetesService kubernetesService;

    @Inject
    public GoogleStorageFactory googleStorageFactory;

    @BeforeMethod
    public void setup() throws Exception {

        final List<Module> modules = new ArrayList<Module>() {{
            add(new KubernetesApiModule());
            add(new GCPModule());
            add(new ExecutorsModule());
        }};

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);

        init();
    }

    @AfterMethod
    public void teardown() throws Exception {
        destroy();
    }

    @Override
    protected void init() throws ApiException, IOException {
        System.setProperty("kubernetes.client", "true");

        assertNotNull(kubernetesService);

        kubernetesService.createSecret(SIDECAR_SECRET_NAME, new HashMap<String, String>() {{
            put("gcp", new String(Files.readAllBytes(Paths.get(System.getProperty("google.application.credentials")))));
        }});
    }

    @Override
    protected void destroy() throws ApiException {
        kubernetesService.deleteSecret(SIDECAR_SECRET_NAME);
        System.setProperty("kubernetes.client", "false");
    }

    @Override
    protected BackupOperationRequest getBackupOperationRequest() {
        final BackupOperationRequest backupOperationRequest = new BackupOperationRequest();
        backupOperationRequest.k8sSecretName = SIDECAR_SECRET_NAME;
        return backupOperationRequest;
    }

    @Override
    public GoogleStorageFactory getGoogleStorageFactory() {
        return googleStorageFactory;
    }

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceTest(inPlaceArguments());
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveCassandraTest(importArguments());
    }

    @Test
    @Ignore
    public void testHardlinkingBackupAndRestore() throws Exception {
        liveCassandraTest(hardlinkingArguments());
    }
}
