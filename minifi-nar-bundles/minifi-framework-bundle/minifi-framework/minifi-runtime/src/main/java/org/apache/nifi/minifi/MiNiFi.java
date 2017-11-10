/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.minifi;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.util.FileUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.transform.TransformerException;
import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// These are from the minifi-nar-utils

public class MiNiFi {

    private static final Logger logger = LoggerFactory.getLogger(MiNiFi.class);
    private final MiNiFiServer minifiServer;
    private final BootstrapListener bootstrapListener;

    public static final String BOOTSTRAP_PORT_PROPERTY = "nifi.bootstrap.listen.port";
    private volatile boolean shutdown = false;

    public static final String PROCESSOR_TAG_NAME = "processor";
    public static final String CONTROLLER_SERVICE_TAG_NAME = "controllerService";
    public static final String REPORTING_TASK_TAG_NAME = "reportingTask";


    private static final Pattern UUID_PATTERN = Pattern.compile("[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}", Pattern.CASE_INSENSITIVE);

    public MiNiFi(final NiFiProperties properties)
            throws ClassNotFoundException, IOException, NoSuchMethodException, InstantiationException,
            IllegalAccessException, IllegalArgumentException, InvocationTargetException, FlowEnrichmentException {
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread t, final Throwable e) {
                logger.error("An Unknown Error Occurred in Thread {}: {}", t, e.toString());
                logger.error("", e);
            }
        });

        // register the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                // shutdown the jetty server
                shutdownHook(true);
            }
        }));

        final String bootstrapPort = System.getProperty(BOOTSTRAP_PORT_PROPERTY);
        if (bootstrapPort != null) {
            try {
                final int port = Integer.parseInt(bootstrapPort);

                if (port < 1 || port > 65535) {
                    throw new RuntimeException("Failed to start MiNiFi because system property '" + BOOTSTRAP_PORT_PROPERTY + "' is not a valid integer in the range 1 - 65535");
                }

                bootstrapListener = new BootstrapListener(this, port);
                bootstrapListener.start();
            } catch (final NumberFormatException nfe) {
                throw new RuntimeException("Failed to start MiNiFi because system property '" + BOOTSTRAP_PORT_PROPERTY + "' is not a valid integer in the range 1 - 65535");
            }
        } else {
            logger.info("MiNiFi started without Bootstrap Port information provided; will not listen for requests from Bootstrap");
            bootstrapListener = null;
        }

        // delete the web working dir - if the application does not start successfully
        // the web app directories might be in an invalid state. when this happens
        // jetty will not attempt to re-extract the war into the directory. by removing
        // the working directory, we can be assured that it will attempt to extract the
        // war every time the application starts.
        File webWorkingDir = properties.getWebWorkingDirectory();
        FileUtils.deleteFilesInDirectory(webWorkingDir, null, logger, true, true);
        FileUtils.deleteFile(webWorkingDir, logger, 3);

        detectTimingIssues();

        // redirect JUL log events
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        // expand the nars
        NarUnpacker.unpackNars(properties);

        // load the extensions classloaders
        NarClassLoaders.getInstance().init(properties.getFrameworkWorkingDirectory(), properties.getExtensionsWorkingDirectory());

        // load the framework classloader
        final ClassLoader frameworkClassLoader = NarClassLoaders.getInstance().getFrameworkBundle().getClassLoader();
        if (frameworkClassLoader == null) {
            throw new IllegalStateException("Unable to find the framework NAR ClassLoader.");
        }

        final Bundle systemBundle = SystemBundle.create(properties);
        final Set<Bundle> narBundles = NarClassLoaders.getInstance().getBundles();

        // discover the extensions
        ExtensionManager.discoverExtensions(systemBundle, narBundles);
        ExtensionManager.logClassLoaderMapping();

        // Enrich the flow xml using the Extension Manager mapping
        final FlowParser flowParser = new FlowParser();
        enrichFlowWithBundleInformation(flowParser, properties);

        // load the server from the framework classloader
        Thread.currentThread().setContextClassLoader(frameworkClassLoader);
        Class<?> minifiServerClass = Class.forName("org.apache.nifi.minifi.MiNiFiServer", true, frameworkClassLoader);
        Constructor<?> minifiServerConstructor = minifiServerClass.getConstructor(NiFiProperties.class);

        final long startTime = System.nanoTime();
        minifiServer = (MiNiFiServer) minifiServerConstructor.newInstance(properties);

        if (shutdown) {
            logger.info("MiNiFi has been shutdown via MiNiFi Bootstrap. Will not start Controller");
        } else {
            minifiServer.start();

            if (bootstrapListener != null) {
                bootstrapListener.sendStartedStatus(true);
            }

            final long endTime = System.nanoTime();
            logger.info("Controller initialization took " + (endTime - startTime) + " nanoseconds.");
        }
    }

    protected void shutdownHook(boolean isReload) {
        try {
            this.shutdown = true;

            logger.info("Initiating shutdown of MiNiFi server...");
            if (minifiServer != null) {
                minifiServer.stop();
            }
            if (bootstrapListener != null) {
                if (isReload) {
                    bootstrapListener.reload();
                } else {
                    bootstrapListener.stop();
                }
            }
            logger.info("MiNiFi server shutdown completed (nicely or otherwise).");
        } catch (final Throwable t) {
            logger.warn("Problem occurred ensuring MiNiFi server was properly terminated due to " + t);
        }
    }

    /**
     * Determine if the machine we're running on has timing issues.
     */
    private void detectTimingIssues() {
        final int minRequiredOccurrences = 25;
        final int maxOccurrencesOutOfRange = 15;
        final AtomicLong lastTriggerMillis = new AtomicLong(System.currentTimeMillis());

        final ScheduledExecutorService service = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = defaultFactory.newThread(r);
                t.setDaemon(true);
                t.setName("Detect Timing Issues");
                return t;
            }
        });

        final AtomicInteger occurrencesOutOfRange = new AtomicInteger(0);
        final AtomicInteger occurrences = new AtomicInteger(0);
        final Runnable command = new Runnable() {
            @Override
            public void run() {
                final long curMillis = System.currentTimeMillis();
                final long difference = curMillis - lastTriggerMillis.get();
                final long millisOff = Math.abs(difference - 2000L);
                occurrences.incrementAndGet();
                if (millisOff > 500L) {
                    occurrencesOutOfRange.incrementAndGet();
                }
                lastTriggerMillis.set(curMillis);
            }
        };

        final ScheduledFuture<?> future = service.scheduleWithFixedDelay(command, 2000L, 2000L, TimeUnit.MILLISECONDS);

        final TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                future.cancel(true);
                service.shutdownNow();

                if (occurrences.get() < minRequiredOccurrences || occurrencesOutOfRange.get() > maxOccurrencesOutOfRange) {
                    logger.warn("MiNiFi has detected that this box is not responding within the expected timing interval, which may cause "
                            + "Processors to be scheduled erratically. Please see the MiNiFi documentation for more information.");
                }
            }
        };
        final Timer timer = new Timer(true);
        timer.schedule(timerTask, 60000L);
    }

    MiNiFiServer getMinifiServer() {
        return minifiServer;
    }

    /**
     * Main entry point of the application.
     *
     * @param args things which are ignored
     */
    public static void main(String[] args) {
        logger.info("Launching MiNiFi...");
        try {
            NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, null);
            new MiNiFi(niFiProperties);
        } catch (final Throwable t) {
            logger.error("Failure to launch MiNiFi due to " + t, t);
        }
    }

    /* Handle Flow bundle enrichment */
    protected static void enrichFlowWithBundleInformation(FlowParser flowParser, NiFiProperties niFiProperties) throws FlowEnrichmentException {
        final Path flowPath = niFiProperties.getFlowConfigurationFile().toPath();
        logger.debug("Enriching generated {} with bundling information", flowPath.toAbsolutePath());

        try {
            final Document flowDocument = flowParser.parse(flowPath.toAbsolutePath().toFile());

            if (flowDocument == null) {
                throw new FlowEnrichmentException("Unable to successfully parse the specified flow at " + flowPath.toAbsolutePath());
            }

            final Map<String, EnrichingElementAdapter> componentDependsUponMap = new HashMap<>();
            // Aggregate all dependency mappings of all component types that need to have a bundle evaluated
            for (String typeElementName : Arrays.asList(PROCESSOR_TAG_NAME, CONTROLLER_SERVICE_TAG_NAME, REPORTING_TASK_TAG_NAME)) {
                final NodeList componentNodeList = flowDocument.getElementsByTagName(typeElementName);
                mapComponents(componentNodeList).forEach((id, metadata) -> componentDependsUponMap.merge(id, metadata, (destination, source) -> {
                    destination.addDependencyIds(source.getDependencyIds());
                    return destination;
                }));
            }

            for (Map.Entry<String, EnrichingElementAdapter> componentEntry : componentDependsUponMap.entrySet()) {

                // If this particular component has already had bundle information applied, skip it
                EnrichingElementAdapter componentToEnrich = componentEntry.getValue();
                if (componentToEnrich.getBundleElement() != null) {
                    continue;
                }

                final EnrichingElementAdapter elementToEnrich = componentToEnrich;
                final String bundleClassLookup = elementToEnrich.getComponentClass();
                final List<Bundle> bundles = ExtensionManager.getBundles(bundleClassLookup);
                BundleCoordinate enrichingBundle = null;
                // If there is only one supporting bundle, choose it, otherwise defer to the other item
                if (bundles.size() == 1) {
                    enrichingBundle = bundles.get(0).getBundleDetails().getCoordinate();
                } else if (bundles.size() > 1) {

                    logger.debug("Found {} bundles for component {}", new Object[]{bundles.size(), bundleClassLookup});
                }

                // If we've made a determination for what bundle this component belongs to, use that to enrich the entry, otherwise, just use the defaults established
                if (enrichingBundle != null) {
                    logger.info("Enriching {} with bundle {}", new Object[]{bundleClassLookup, enrichingBundle == null ? "null bundle" : enrichingBundle.getCoordinate()});
                    // Adjust the bundle to reflect the values we learned from the Extension Manager
                    elementToEnrich.setBundleInformation(enrichingBundle);

                    // if we have dependent component IDs, iterate through those and update them with the appropriate version
                    if (!componentDependsUponMap.get(componentEntry.getKey()).getDependencyIds().isEmpty()) {
                        for (String dependentId : componentToEnrich.getDependencyIds()) {
                            final EnrichingElementAdapter dependentComponentAdapter = componentDependsUponMap.get(dependentId);
                            final String dependentComponentClass = dependentComponentAdapter.getComponentClass();
                            final List<Bundle> dependentBundles = ExtensionManager.getBundles(dependentComponentClass);
                            if (dependentBundles.size() == 1) {
                                final BundleCoordinate bundle = dependentBundles.get(0).getBundleDetails().getCoordinate();
                                if (!bundle.getVersion().equalsIgnoreCase(enrichingBundle.getVersion())) {
                                    throw new FlowEnrichmentException("Only bundle provided as a dependency for " + dependentComponentClass + " is not an appropriate version.");
                                }
                                dependentComponentAdapter.setBundleInformation(enrichingBundle);
                            } else if (dependentBundles.size() > 1) {
                                List<Bundle> matchingBundles = dependentBundles.stream()
                                        .filter(bundle ->
                                                bundle.getBundleDetails().getCoordinate().getVersion().equalsIgnoreCase(
                                                        componentToEnrich.getBundleElement().getElementsByTagName("version").item(0).getTextContent()))
                                        .collect(Collectors.toList());
                                if (matchingBundles.isEmpty()) {
                                    throw new FlowEnrichmentException("Could not find a bundle to act as a valid dependency for " + bundleClassLookup);
                                }
                                BundleCoordinate dependencyBundleCoordinate = matchingBundles.get(0).getBundleDetails().getCoordinate();
                                dependentComponentAdapter.setBundleInformation(dependencyBundleCoordinate);
                            }
                            logger.debug("Found dependent component {} for {}", new Object[]{dependentComponentClass, componentToEnrich.getComponentClass()});
                        }
                    }
                }
            }

            flowParser.writeFlow(flowDocument, flowPath.toAbsolutePath());
        } catch (IOException | TransformerException e) {
            throw new FlowEnrichmentException("Unable to successfully automate the enrichment of the generated flow with bundle information", e);
        }
    }

    /**
     * Find dependent components for the nodes provided.
     * <p>
     * We do not have any other information in a generic sense other than that the  properties that make use of UUIDs
     * are eligible to be dependent components; there is no typing that a value is an ID and not just the format of a UUID.
     * If we find a property that has a UUID as its value, we take note and create a mapping.
     * If it is a valid ID of another component, we can use this to pair up versions, otherwise, it is ignored.
     *
     * @param parentNodes component nodes to map to dependent components (e.g. Processor -> Controller Service)
     * @return a map of component IDs to their metadata about their relationship
     */
    protected static Map<String, EnrichingElementAdapter> mapComponents(NodeList parentNodes) {

        final Map<String, EnrichingElementAdapter> componentReferenceMap = new HashMap<>();

        for (int compIdx = 0; compIdx < parentNodes.getLength(); compIdx++) {
            final Set<String> referencingComponentIds = new HashSet<>();
            Node subjComponent = parentNodes.item(compIdx);
            final EnrichingElementAdapter enrichingElement = new EnrichingElementAdapter((Element) subjComponent);

            for (Element property : enrichingElement.getProperties()) {
                NodeList valueElements = property.getElementsByTagName("value");
                if (valueElements.getLength() > 0) {
                    final String value = valueElements.item(0).getTextContent();
                    if (UUID_PATTERN.matcher(value).matches()) {
                        referencingComponentIds.add(value);
                    }
                }
            }
            enrichingElement.addDependencyIds(referencingComponentIds);
            componentReferenceMap.put(enrichingElement.getComponentId(), enrichingElement);
        }
        return componentReferenceMap;
    }


    /*
     * Convenience class to aid in interacting with the XML elements pertaining to a bundle-able component
     */
    private static class EnrichingElementAdapter {
        public static final String BUNDLE_ELEMENT_NAME = "bundle";

        public static final String GROUP_ELEMENT_NAME = "group";
        public static final String ARTIFACT_ELEMENT_NAME = "artifact";
        public static final String VERSION_ELEMENT_NAME = "version";

        public static final String PROPERTY_ELEMENT_NAME = "property";

        // Source object
        private Element rawElement;

        // Metadata
        private String id;
        private String compClass;
        private Element bundleElement;
        private Set<String> dependencyIds = new HashSet<>();

        public EnrichingElementAdapter(Element element) {
            this.rawElement = element;
        }

        public String getComponentId() {
            if (this.id == null) {
                this.id = lookupValue("id");
            }
            return this.id;
        }

        public String getComponentClass() {
            if (this.compClass == null) {
                this.compClass = lookupValue("class");
            }
            return compClass;
        }

        public Element getBundleElement() {
            return this.bundleElement;
        }

        public List<Element> getProperties() {
            return FlowParser.getChildrenByTagName(this.rawElement, PROPERTY_ELEMENT_NAME);
        }

        private String lookupValue(String elementName) {
            return FlowParser.getChildrenByTagName(this.rawElement, elementName).get(0).getTextContent();
        }

        public void setBundleInformation(final BundleCoordinate bundleCoordinate) {
            // If we are handling a component that does not yet have bundle information, create a placeholder element
            if (this.bundleElement == null) {
                this.bundleElement = this.rawElement.getOwnerDocument().createElement(BUNDLE_ELEMENT_NAME);
                for (String elementTag : Arrays.asList(GROUP_ELEMENT_NAME, ARTIFACT_ELEMENT_NAME, VERSION_ELEMENT_NAME)) {
                    this.bundleElement.appendChild(this.bundleElement.getOwnerDocument().createElement(elementTag));
                }
                this.rawElement.appendChild(this.bundleElement);
            }

            this.bundleElement.getElementsByTagName(GROUP_ELEMENT_NAME).item(0).setTextContent(bundleCoordinate.getGroup());
            this.bundleElement.getElementsByTagName(ARTIFACT_ELEMENT_NAME).item(0).setTextContent(bundleCoordinate.getId());
            this.bundleElement.getElementsByTagName(VERSION_ELEMENT_NAME).item(0).setTextContent(bundleCoordinate.getVersion());
        }

        public Set<String> getDependencyIds() {
            return dependencyIds;
        }

        public void addDependencyId(String dependencyId) {
            this.dependencyIds.add(dependencyId);
        }

        public void addDependencyIds(Collection<String> dependencyIds) {
            this.dependencyIds.addAll(dependencyIds);
        }
    }
}
