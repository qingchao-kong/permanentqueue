package cn.pockethub.permanentqueue.kafka.server;

import cn.pockethub.permanentqueue.kafka.log.LogCleaner;
import cn.pockethub.permanentqueue.kafka.utils.Logging;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Dynamic broker configurations are stored in ZooKeeper and may be defined at two levels:
 * <ul>
 *   <li>Per-broker configs persisted at <tt>/configs/brokers/{brokerId}</tt>: These can be described/altered
 *       using AdminClient using the resource name brokerId.</li>
 *   <li>Cluster-wide defaults persisted at <tt>/configs/brokers/&lt;default&gt;</tt>: These can be described/altered
 *       using AdminClient using an empty resource name.</li>
 * </ul>
 * The order of precedence for broker configs is:
 * <ol>
 *   <li>DYNAMIC_BROKER_CONFIG: stored in ZK at /configs/brokers/{brokerId}</li>
 *   <li>DYNAMIC_DEFAULT_BROKER_CONFIG: stored in ZK at /configs/brokers/&lt;default&gt;</li>
 *   <li>STATIC_BROKER_CONFIG: properties that broker is started up with, typically from server.properties file</li>
 *   <li>DEFAULT_CONFIG: Default configs defined in KafkaConfig</li>
 * </ol>
 * Log configs use topic config overrides if defined and fallback to broker defaults using the order of precedence above.
 * Topic config overrides may use a different config name from the default broker config.
 * See [[kafka.log.LogConfig#TopicConfigSynonyms]] for the mapping.
 * <p>
 * AdminClient returns all config synonyms in the order of precedence when configs are described with
 * <code>includeSynonyms</code>. In addition to configs that may be defined with the same name at different levels,
 * some configs have additional synonyms.
 * </p>
 * <ul>
 *   <li>Listener configs may be defined using the prefix <tt>listener.name.{listenerName}.{configName}</tt>. These may be
 *       configured as dynamic or static broker configs. Listener configs have higher precedence than the base configs
 *       that don't specify the listener name. Listeners without a listener config use the base config. Base configs
 *       may be defined only as STATIC_BROKER_CONFIG or DEFAULT_CONFIG and cannot be updated dynamically.<li>
 *   <li>Some configs may be defined using multiple properties. For example, <tt>log.roll.ms</tt> and
 *       <tt>log.roll.hours</tt> refer to the same config that may be defined in milliseconds or hours. The order of
 *       precedence of these synonyms is described in the docs of these configs in [[kafka.server.KafkaConfig]].</li>
 * </ul>
 */
public class DynamicBrokerConfig extends Logging {

    protected static Set<String> DynamicSecurityConfigs = SslConfigs.RECONFIGURABLE_CONFIGS;

    private static Set<String> AllDynamicConfigs = new HashSet<>();

    static {
        AllDynamicConfigs.addAll(DynamicSecurityConfigs);
        AllDynamicConfigs.addAll(LogCleaner.ReconfigurableConfigs);
        AllDynamicConfigs.addAll(DynamicLogConfig.ReconfigurableConfigs);
//        AllDynamicConfigs.addAll(DynamicThreadPool.ReconfigurableConfigs);
        AllDynamicConfigs.add(KafkaConfig.MetricReporterClassesProp);
//        AllDynamicConfigs.addAll(DynamicListenerConfig.ReconfigurableConfigs);
//        AllDynamicConfigs.addAll(SocketServer.ReconfigurableConfigs);
    }

    private static Set<String> ClusterLevelListenerConfigs = new HashSet<>(Arrays.asList(KafkaConfig.MaxConnectionsProp,
            KafkaConfig.MaxConnectionCreationRateProp,
            KafkaConfig.NumNetworkThreadsProp));

    private static Set<String> PerBrokerConfigs = new HashSet<>();

    static {
        PerBrokerConfigs.addAll(DynamicSecurityConfigs);
//        PerBrokerConfigs.addAll(DynamicListenerConfig.ReconfigurableConfigs);
        PerBrokerConfigs.removeAll(ClusterLevelListenerConfigs);
    }

    private static Set<String> ListenerMechanismConfigs = new HashSet<>(Arrays.asList(KafkaConfig.SaslJaasConfigProp,
            KafkaConfig.SaslLoginCallbackHandlerClassProp,
            KafkaConfig.SaslLoginClassProp,
            KafkaConfig.SaslServerCallbackHandlerClassProp,
            KafkaConfig.ConnectionsMaxReauthMsProp));

    private static Set<String> ReloadableFileConfigs = new HashSet<>(Arrays.asList(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));

//    public static Pattern ListenerConfigRegex = Pattern.compile("listener\.name\.[^.]*\.(.*)");

    private static Set<String> DynamicPasswordConfigs = new HashSet<>();

    static {
        Set<String> passwordConfigs = KafkaConfig.configKeys().entrySet().stream()
                .filter(entry -> entry.getValue().type() == ConfigDef.Type.PASSWORD)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        DynamicPasswordConfigs = AllDynamicConfigs.stream().filter(passwordConfigs::contains).collect(Collectors.toSet());
    }

    private KafkaConfig kafkaConfig;

    protected Map<String, String> staticBrokerConfigs ;
    protected Map<String, String>  staticDefaultConfigs = ConfigDef.convertToStringMapWithPasswordValues(KafkaConfig.defaultValues());
    private Map<String, String> dynamicBrokerConfigs = new HashMap<>();
    private Map<String, String> dynamicDefaultConfigs = new HashMap<>();

    // Use COWArrayList to prevent concurrent modification exception when an item is added by one thread to these
    // collections, while another thread is iterating over them.
    private CopyOnWriteArrayList<Reconfigurable> reconfigurables = new CopyOnWriteArrayList();
    private CopyOnWriteArrayList<BrokerReconfigurable> brokerReconfigurables = new CopyOnWriteArrayList();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private KafkaConfig currentConfig = null;
//    private Optional<PasswordEncoder> dynamicConfigPasswordEncoder=CollectionUtils.isEmpty(kafkaConfig.getProcessRoles())?
//            maybeCreatePasswordEncoder(kafkaConfig.passwordEncoderSecret):Optional.of(PasswordEncoder.noop());
//    private Optional<PasswordEncoder> dynamicConfigPasswordEncoder = if (kafkaConfig.processRoles().isEmpty) {
//        maybeCreatePasswordEncoder(kafkaConfig.passwordEncoderSecret)
//    } else {
//        Some(PasswordEncoder.noop())
//    }

    public DynamicBrokerConfig(KafkaConfig kafkaConfig){
        this.kafkaConfig=kafkaConfig;
        this.staticBrokerConfigs=ConfigDef.convertToStringMapWithPasswordValues(kafkaConfig.originalsFromThisConfig());
    }

//    protected void initialize(Optional<KafkaZkClient> zkClientOpt) {
//        currentConfig = new KafkaConfig(kafkaConfig.getProps(), false, Optional.empty());
//
//        zkClientOpt.ifPresent(zkClient->{
//            AdminZkClient adminZkClient = new AdminZkClient(zkClient);
//            updateDefaultConfig(adminZkClient.fetchEntityConfig(ConfigType.Broker, ConfigEntityName.Default), false);
//            Properties props = adminZkClient.fetchEntityConfig(ConfigType.Broker, kafkaConfig.brokerId.toString());
//            Properties brokerConfig = maybeReEncodePasswords(props, adminZkClient);
//            updateBrokerConfig(kafkaConfig.brokerId, brokerConfig);
//        });
//    }

    /**
     * Clear all cached values. This is used to clear state on broker shutdown to avoid
     * exceptions in tests when broker is restarted. These fields are re-initialized when
     * broker starts up.
     */
    protected void clear(){
        dynamicBrokerConfigs.clear();
        dynamicDefaultConfigs.clear();
        reconfigurables.clear();
        brokerReconfigurables.clear();
    }

    /**
     * Add reconfigurables to be notified when a dynamic broker config is updated.
     *
     * `Reconfigurable` is the public API used by configurable plugins like metrics reporter
     * and quota callbacks. These are reconfigured before `KafkaConfig` is updated so that
     * the update can be aborted if `reconfigure()` fails with an exception.
     *
     * `BrokerReconfigurable` is used for internal reconfigurable classes. These are
     * reconfigured after `KafkaConfig` is updated so that they can access `KafkaConfig`
     * directly. They are provided both old and new configs.
     */
//    public void addReconfigurables(KafkaBroker kafkaServer) {
//        kafkaServer.authorizer match {
//            case Some(authz: Reconfigurable) => addReconfigurable(authz)
//            case _ =>
//        }
//        addReconfigurable(kafkaServer.kafkaYammerMetrics)
//        addReconfigurable(new DynamicMetricsReporters(kafkaConfig.brokerId, kafkaServer.config, kafkaServer.metrics, kafkaServer.clusterId))
//        addReconfigurable(new DynamicClientQuotaCallback(kafkaServer))
//
//        addBrokerReconfigurable(new DynamicThreadPool(kafkaServer))
//        addBrokerReconfigurable(new DynamicLogConfig(kafkaServer.logManager, kafkaServer))
//        addBrokerReconfigurable(new DynamicListenerConfig(kafkaServer))
//        addBrokerReconfigurable(kafkaServer.socketServer)
//    }
//
//    def addReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
//        verifyReconfigurableConfigs(reconfigurable.reconfigurableConfigs.asScala)
//        reconfigurables.add(reconfigurable)
//    }
//
//    def addBrokerReconfigurable(reconfigurable: BrokerReconfigurable): Unit = CoreUtils.inWriteLock(lock) {
//        verifyReconfigurableConfigs(reconfigurable.reconfigurableConfigs)
//        brokerReconfigurables.add(reconfigurable)
//    }
//
//    def removeReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
//        reconfigurables.remove(reconfigurable)
//    }
//
//    private def verifyReconfigurableConfigs(configNames: Set[String]): Unit = CoreUtils.inWriteLock(lock) {
//        val nonDynamic = configNames.filter(DynamicConfig.Broker.nonDynamicProps.contains)
//        require(nonDynamic.isEmpty, s"Reconfigurable contains non-dynamic configs $nonDynamic")
//    }
//
//    // Visibility for testing
//    private[server] def currentKafkaConfig: KafkaConfig = CoreUtils.inReadLock(lock) {
//        currentConfig
//    }
//
//    private[server] def currentDynamicBrokerConfigs: Map[String, String] = CoreUtils.inReadLock(lock) {
//        dynamicBrokerConfigs.clone()
//    }
//
//    private[server] def currentDynamicDefaultConfigs: Map[String, String] = CoreUtils.inReadLock(lock) {
//        dynamicDefaultConfigs.clone()
//    }
//
//    private[server] def updateBrokerConfig(brokerId: Int, persistentProps: Properties, doLog: Boolean = true): Unit = CoreUtils.inWriteLock(lock) {
//        try {
//            val props = fromPersistentProps(persistentProps, perBrokerConfig = true)
//            dynamicBrokerConfigs.clear()
//            dynamicBrokerConfigs ++= props.asScala
//            updateCurrentConfig(doLog)
//        } catch {
//            case e: Exception => error(s"Per-broker configs of $brokerId could not be applied: ${persistentProps.keys()}", e)
//        }
//    }
//
//    private[server] def updateDefaultConfig(persistentProps: Properties, doLog: Boolean = true): Unit = CoreUtils.inWriteLock(lock) {
//        try {
//            val props = fromPersistentProps(persistentProps, perBrokerConfig = false)
//            dynamicDefaultConfigs.clear()
//            dynamicDefaultConfigs ++= props.asScala
//            updateCurrentConfig(doLog)
//        } catch {
//            case e: Exception => error(s"Cluster default configs could not be applied: ${persistentProps.keys()}", e)
//        }
//    }
//
//    /**
//     * All config updates through ZooKeeper are triggered through actual changes in values stored in ZooKeeper.
//     * For some configs like SSL keystores and truststores, we also want to reload the store if it was modified
//     * in-place, even though the actual value of the file path and password haven't changed. This scenario alone
//     * is handled here when a config update request using admin client is processed by ZkAdminManager. If any of
//     * the SSL configs have changed, then the update will not be done here, but will be handled later when ZK
//     * changes are processed. At the moment, only listener configs are considered for reloading.
//     */
//    private[server] def reloadUpdatedFilesWithoutConfigChange(newProps: Properties): Unit = CoreUtils.inWriteLock(lock) {
//        reconfigurables.asScala
//                .filter(reconfigurable => ReloadableFileConfigs.exists(reconfigurable.reconfigurableConfigs.contains))
//      .foreach {
//            case reconfigurable: ListenerReconfigurable =>
//                val kafkaProps = validatedKafkaProps(newProps, perBrokerConfig = true)
//                val newConfig = new KafkaConfig(kafkaProps.asJava, false, None)
//                processListenerReconfigurable(reconfigurable, newConfig, Collections.emptyMap(), validateOnly = false, reloadOnly = true)
//            case reconfigurable =>
//                trace(s"Files will not be reloaded without config change for $reconfigurable")
//        }
//    }
//
//    private def maybeCreatePasswordEncoder(secret: Option[Password]): Option[PasswordEncoder] = {
//        secret.map { secret =>
//            PasswordEncoder.encrypting(secret,
//                    kafkaConfig.passwordEncoderKeyFactoryAlgorithm,
//                    kafkaConfig.passwordEncoderCipherAlgorithm,
//                    kafkaConfig.passwordEncoderKeyLength,
//                    kafkaConfig.passwordEncoderIterations)
//        }
//    }
//
//    private def passwordEncoder: PasswordEncoder = {
//        dynamicConfigPasswordEncoder.getOrElse(throw new ConfigException("Password encoder secret not configured"))
//    }
//
//    private[server] def toPersistentProps(configProps: Properties, perBrokerConfig: Boolean): Properties = {
//        val props = configProps.clone().asInstanceOf[Properties]
//
//        def encodePassword(configName: String, value: String): Unit = {
//        if (value != null) {
//            if (!perBrokerConfig)
//                throw new ConfigException("Password config can be defined only at broker level")
//            props.setProperty(configName, passwordEncoder.encode(new Password(value)))
//        }
//    }
//        configProps.asScala.forKeyValue { (name, value) =>
//            if (isPasswordConfig(name))
//                encodePassword(name, value)
//        }
//        props
//    }
//
//    private[server] def fromPersistentProps(persistentProps: Properties,
//                                            perBrokerConfig: Boolean): Properties = {
//        val props = persistentProps.clone().asInstanceOf[Properties]
//
//        // Remove all invalid configs from `props`
//        removeInvalidConfigs(props, perBrokerConfig)
//        def removeInvalidProps(invalidPropNames: Set[String], errorMessage: String): Unit = {
//        if (invalidPropNames.nonEmpty) {
//            invalidPropNames.foreach(props.remove)
//            error(s"$errorMessage: $invalidPropNames")
//        }
//    }
//        removeInvalidProps(nonDynamicConfigs(props), "Non-dynamic configs configured in ZooKeeper will be ignored")
//        removeInvalidProps(securityConfigsWithoutListenerPrefix(props),
//                "Security configs can be dynamically updated only using listener prefix, base configs will be ignored")
//        if (!perBrokerConfig)
//            removeInvalidProps(perBrokerConfigs(props), "Per-broker configs defined at default cluster level will be ignored")
//
//        def decodePassword(configName: String, value: String): Unit = {
//        if (value != null) {
//            try {
//                props.setProperty(configName, passwordEncoder.decode(value).value)
//            } catch {
//                case e: Exception =>
//                    error(s"Dynamic password config $configName could not be decoded, ignoring.", e)
//                    props.remove(configName)
//            }
//        }
//    }
//
//        props.asScala.forKeyValue { (name, value) =>
//            if (isPasswordConfig(name))
//                decodePassword(name, value)
//        }
//        props
//    }
//
//    // If the secret has changed, password.encoder.old.secret contains the old secret that was used
//    // to encode the configs in ZK. Decode passwords using the old secret and update ZK with values
//    // encoded using the current secret. Ignore any errors during decoding since old secret may not
//    // have been removed during broker restart.
//    private def maybeReEncodePasswords(persistentProps: Properties, adminZkClient: AdminZkClient): Properties = {
//        val props = persistentProps.clone().asInstanceOf[Properties]
//        if (props.asScala.keySet.exists(isPasswordConfig)) {
//            maybeCreatePasswordEncoder(kafkaConfig.passwordEncoderOldSecret).foreach { passwordDecoder =>
//                persistentProps.asScala.forKeyValue { (configName, value) =>
//                    if (isPasswordConfig(configName) && value != null) {
//                        val decoded = try {
//                            Some(passwordDecoder.decode(value).value)
//                        } catch {
//                            case _: Exception =>
//                                debug(s"Dynamic password config $configName could not be decoded using old secret, new secret will be used.")
//                                None
//                        }
//                        decoded.foreach(value => props.put(configName, passwordEncoder.encode(new Password(value))))
//                    }
//                }
//                adminZkClient.changeBrokerConfig(Some(kafkaConfig.brokerId), props)
//            }
//        }
//        props
//    }
//
//    /**
//     * Validate the provided configs `propsOverride` and return the full Kafka configs with
//     * the configured defaults and these overrides.
//     *
//     * Note: The caller must acquire the read or write lock before invoking this method.
//     */
//    private def validatedKafkaProps(propsOverride: Properties, perBrokerConfig: Boolean): Map[String, String] = {
//        val propsResolved = DynamicBrokerConfig.resolveVariableConfigs(propsOverride)
//        validateConfigs(propsResolved, perBrokerConfig)
//        val newProps = mutable.Map[String, String]()
//        newProps ++= staticBrokerConfigs
//        if (perBrokerConfig) {
//            overrideProps(newProps, dynamicDefaultConfigs)
//            overrideProps(newProps, propsResolved.asScala)
//        } else {
//            overrideProps(newProps, propsResolved.asScala)
//            overrideProps(newProps, dynamicBrokerConfigs)
//        }
//        newProps
//    }
//
//    private[server] def validate(props: Properties, perBrokerConfig: Boolean): Unit = CoreUtils.inReadLock(lock) {
//        val newProps = validatedKafkaProps(props, perBrokerConfig)
//        processReconfiguration(newProps, validateOnly = true)
//    }
//
//    private def removeInvalidConfigs(props: Properties, perBrokerConfig: Boolean): Unit = {
//        try {
//            validateConfigTypes(props)
//            props.asScala
//        } catch {
//            case e: Exception =>
//                val invalidProps = props.asScala.filter { case (k, v) =>
//                val props1 = new Properties
//                props1.put(k, v)
//                try {
//                    validateConfigTypes(props1)
//                    false
//                } catch {
//                case _: Exception => true
//            }
//            }
//            invalidProps.keys.foreach(props.remove)
//            val configSource = if (perBrokerConfig) "broker" else "default cluster"
//            error(s"Dynamic $configSource config contains invalid values in: ${invalidProps.keys}, these configs will be ignored", e)
//        }
//    }
//
//    private[server] def maybeReconfigure(reconfigurable: Reconfigurable, oldConfig: KafkaConfig, newConfig: util.Map[String, _]): Unit = {
//        if (reconfigurable.reconfigurableConfigs.asScala.exists(key => oldConfig.originals.get(key) != newConfig.get(key)))
//        reconfigurable.reconfigure(newConfig)
//    }
//
//    /**
//     * Returns the change in configurations between the new props and current props by returning a
//     * map of the changed configs, as well as the set of deleted keys
//     */
//    private def updatedConfigs(newProps: java.util.Map[String, _],
//                               currentProps: java.util.Map[String, _]): (mutable.Map[String, _], Set[String]) = {
//        val changeMap = newProps.asScala.filter {
//            case (k, v) => v != currentProps.get(k)
//        }
//        val deletedKeySet = currentProps.asScala.filter {
//            case (k, _) => !newProps.containsKey(k)
//        }.keySet
//                (changeMap, deletedKeySet)
//    }
//
//    /**
//     * Updates values in `props` with the new values from `propsOverride`. Synonyms of updated configs
//     * are removed from `props` to ensure that the config with the higher precedence is applied. For example,
//     * if `log.roll.ms` was defined in server.properties and `log.roll.hours` is configured dynamically,
//     * `log.roll.hours` from the dynamic configuration will be used and `log.roll.ms` will be removed from
//     * `props` (even though `log.roll.hours` is secondary to `log.roll.ms`).
//     */
//    private def overrideProps(props: mutable.Map[String, String], propsOverride: mutable.Map[String, String]): Unit = {
//        propsOverride.forKeyValue { (k, v) =>
//            // Remove synonyms of `k` to ensure the right precedence is applied. But disable `matchListenerOverride`
//            // so that base configs corresponding to listener configs are not removed. Base configs should not be removed
//            // since they may be used by other listeners. It is ok to retain them in `props` since base configs cannot be
//            // dynamically updated and listener-specific configs have the higher precedence.
//            brokerConfigSynonyms(k, matchListenerOverride = false).foreach(props.remove)
//            props.put(k, v)
//        }
//    }
//
//    private def updateCurrentConfig(doLog: Boolean): Unit = {
//        val newProps = mutable.Map[String, String]()
//        newProps ++= staticBrokerConfigs
//        overrideProps(newProps, dynamicDefaultConfigs)
//        overrideProps(newProps, dynamicBrokerConfigs)
//
//        val oldConfig = currentConfig
//        val (newConfig, brokerReconfigurablesToUpdate) = processReconfiguration(newProps, validateOnly = false, doLog)
//        if (newConfig ne currentConfig) {
//            currentConfig = newConfig
//            kafkaConfig.updateCurrentConfig(newConfig)
//
//            // Process BrokerReconfigurable updates after current config is updated
//            brokerReconfigurablesToUpdate.foreach(_.reconfigure(oldConfig, newConfig))
//        }
//    }
//
//    private def processReconfiguration(newProps: Map[String, String], validateOnly: Boolean, doLog: Boolean = false): (KafkaConfig, List[BrokerReconfigurable]) = {
//        val newConfig = new KafkaConfig(newProps.asJava, doLog, None)
//        val (changeMap, deletedKeySet) = updatedConfigs(newConfig.originalsFromThisConfig, currentConfig.originals)
//        if (changeMap.nonEmpty || deletedKeySet.nonEmpty) {
//            try {
//                val customConfigs = new util.HashMap[String, Object](newConfig.originalsFromThisConfig) // non-Kafka configs
//                        newConfig.valuesFromThisConfig.keySet.forEach(k => customConfigs.remove(k))
//                reconfigurables.forEach {
//                    case listenerReconfigurable: ListenerReconfigurable =>
//                        processListenerReconfigurable(listenerReconfigurable, newConfig, customConfigs, validateOnly, reloadOnly = false)
//                    case reconfigurable =>
//                        if (needsReconfiguration(reconfigurable.reconfigurableConfigs, changeMap.keySet, deletedKeySet))
//                            processReconfigurable(reconfigurable, changeMap.keySet, newConfig.valuesFromThisConfig, customConfigs, validateOnly)
//                }
//
//                // BrokerReconfigurable updates are processed after config is updated. Only do the validation here.
//                val brokerReconfigurablesToUpdate = mutable.Buffer[BrokerReconfigurable]()
//                brokerReconfigurables.forEach { reconfigurable =>
//                    if (needsReconfiguration(reconfigurable.reconfigurableConfigs.asJava, changeMap.keySet, deletedKeySet)) {
//                        reconfigurable.validateReconfiguration(newConfig)
//                        if (!validateOnly)
//                            brokerReconfigurablesToUpdate += reconfigurable
//                    }
//                }
//                (newConfig, brokerReconfigurablesToUpdate.toList)
//            } catch {
//                case e: Exception =>
//                    if (!validateOnly)
//                        error(s"Failed to update broker configuration with configs : " +
//                                s"${ConfigUtils.configMapToRedactedString(newConfig.originalsFromThisConfig, KafkaConfig.configDef)}", e)
//                    throw new ConfigException("Invalid dynamic configuration", e)
//            }
//        }
//        else
//            (currentConfig, List.empty)
//    }
//
//    private def needsReconfiguration(reconfigurableConfigs: util.Set[String], updatedKeys: Set[String], deletedKeys: Set[String]): Boolean = {
//        reconfigurableConfigs.asScala.intersect(updatedKeys).nonEmpty ||
//                reconfigurableConfigs.asScala.intersect(deletedKeys).nonEmpty
//    }
//
//    private def processListenerReconfigurable(listenerReconfigurable: ListenerReconfigurable,
//                                              newConfig: KafkaConfig,
//                                              customConfigs: util.Map[String, Object],
//                                              validateOnly: Boolean,
//                                              reloadOnly:  Boolean): Unit = {
//        val listenerName = listenerReconfigurable.listenerName
//        val oldValues = currentConfig.valuesWithPrefixOverride(listenerName.configPrefix)
//        val newValues = newConfig.valuesFromThisConfigWithPrefixOverride(listenerName.configPrefix)
//        val (changeMap, deletedKeys) = updatedConfigs(newValues, oldValues)
//        val updatedKeys = changeMap.keySet
//        val configsChanged = needsReconfiguration(listenerReconfigurable.reconfigurableConfigs, updatedKeys, deletedKeys)
//        // if `reloadOnly`, reconfigure if configs haven't changed. Otherwise reconfigure if configs have changed
//        if (reloadOnly != configsChanged)
//            processReconfigurable(listenerReconfigurable, updatedKeys, newValues, customConfigs, validateOnly)
//    }
//
//    private def processReconfigurable(reconfigurable: Reconfigurable,
//                                      updatedConfigNames: Set[String],
//                                      allNewConfigs: util.Map[String, _],
//                                      newCustomConfigs: util.Map[String, Object],
//                                      validateOnly: Boolean): Unit = {
//        val newConfigs = new util.HashMap[String, Object]
//        allNewConfigs.forEach((k, v) => newConfigs.put(k, v.asInstanceOf[AnyRef]))
//        newConfigs.putAll(newCustomConfigs)
//        try {
//            reconfigurable.validateReconfiguration(newConfigs)
//        } catch {
//            case e: ConfigException => throw e
//            case _: Exception =>
//                throw new ConfigException(s"Validation of dynamic config update of $updatedConfigNames failed with class ${reconfigurable.getClass}")
//        }
//
//        if (!validateOnly) {
//            info(s"Reconfiguring $reconfigurable, updated configs: $updatedConfigNames " +
//                    s"custom configs: ${ConfigUtils.configMapToRedactedString(newCustomConfigs, KafkaConfig.configDef)}")
//            reconfigurable.reconfigure(newConfigs)
//        }
//    }

    public static Boolean isPasswordConfig(String name) {
        return DynamicBrokerConfig.DynamicPasswordConfigs.stream().anyMatch(name::endsWith);
    }

//    public static List<String> brokerConfigSynonyms(String name, Boolean matchListenerOverride) {
//        switch (name) {
//            case KafkaConfig.LogRollTimeMillisProp:
//            case KafkaConfig.LogRollTimeHoursProp:
//                return Arrays.asList(KafkaConfig.LogRollTimeMillisProp, KafkaConfig.LogRollTimeHoursProp);
//            case KafkaConfig.LogRollTimeJitterMillisProp:
//            case KafkaConfig.LogRollTimeJitterHoursProp:
//                return Arrays.asList(KafkaConfig.LogRollTimeJitterMillisProp, KafkaConfig.LogRollTimeJitterHoursProp);
//            case KafkaConfig.LogFlushIntervalMsProp: // LogFlushSchedulerIntervalMsProp is used as default
//                return Arrays.asList(KafkaConfig.LogFlushIntervalMsProp, KafkaConfig.LogFlushSchedulerIntervalMsProp);
//            case KafkaConfig.LogRetentionTimeMillisProp:
//            case KafkaConfig.LogRetentionTimeMinutesProp:
//            case KafkaConfig.LogRetentionTimeHoursProp:
//                return Arrays.asList(KafkaConfig.LogRetentionTimeMillisProp, KafkaConfig.LogRetentionTimeMinutesProp, KafkaConfig.LogRetentionTimeHoursProp);
//            default:
//                if (ListenerConfigRegex(baseName) && matchListenerOverride) {
//                    // `ListenerMechanismConfigs` are specified as listenerPrefix.mechanism.<configName>
//                    // and other listener configs are specified as listenerPrefix.<configName>
//                    // Add <configName> as a synonym in both cases.
//                    val mechanismConfig = ListenerMechanismConfigs.find(baseName.endsWith);
//                    return Arrays.asList(name, mechanismConfig.getOrElse(baseName));
//                } else {
//                    //default
//                    return Arrays.asList(name);
//                }
//                break;
//        }
//
//    }

//    public static void validateConfigs(Properties props, Boolean perBrokerConfig) {
//
//        checkInvalidProps(nonDynamicConfigs(props), "Cannot update these configs dynamically");
//        checkInvalidProps(securityConfigsWithoutListenerPrefix(props),
//                "These security configs can be dynamically updated only per-listener using the listener prefix");
//        validateConfigTypes(props);
//        if (!perBrokerConfig) {
//            checkInvalidProps(perBrokerConfigs(props),
//                    "Cannot update these configs at default cluster level, broker id must be specified");
//        }
//    }

    private static void checkInvalidProps(Set<String> invalidPropNames, String errorMessage) {
        if (CollectionUtils.isNotEmpty(invalidPropNames)) {
            throw new ConfigException(String.format("%s: %s", errorMessage, invalidPropNames));
        }
    }
//
//    private static Set<String> perBrokerConfigs(Properties props) {
//        Set<Object> configNames = props.keySet();
//
//        Set<String> configs = configNames.stream()
//                .map(Object::toString)
//                .filter(o -> PerBrokerConfigs.contains(o))
//                .collect(Collectors.toSet());
//
//        Set<String> collect = configNames.stream()
//                .map(Object::toString)
//                .filter(DynamicBrokerConfig::perBrokerListenerConfig)
//                .collect(Collectors.toSet());
//
//        configs.addAll(collect);
//        return configs;
//    }

//    private static Boolean perBrokerListenerConfig(String name) {
//        name match {
//            case ListenerConfigRegex(baseName) =>!ClusterLevelListenerConfigs.contains(baseName)
//            case _ =>false
//        }
//    }
//
//    private static Set<String> nonDynamicConfigs(Properties props) {
//        return props.keySet().intersect(DynamicConfig.Broker.nonDynamicProps);
//    }
//
//    private static Set<String> securityConfigsWithoutListenerPrefix(Properties props) {
//        return DynamicSecurityConfigs.stream().filter(props::containsKey).collect(Collectors.toSet());
//    }
//
//    private static void validateConfigTypes(Properties props) {
//        Properties baseProps = new Properties();
//        props.asScala.foreach {
//            case (ListenerConfigRegex(baseName), v) =>baseProps.put(baseName, v)
//            case (k, v) =>baseProps.put(k, v)
//        }
//        DynamicConfig.Broker.validate(baseProps);
//    }

    protected static void addDynamicConfigs(ConfigDef configDef) {
        for (Map.Entry<String, ConfigDef.ConfigKey> entry : KafkaConfig.configKeys().entrySet()) {
            String configName = entry.getKey();
            ConfigDef.ConfigKey config = entry.getValue();
            if (AllDynamicConfigs.contains(configName)) {
                configDef.define(config.name, config.type, config.defaultValue, config.validator,
                        config.importance, config.documentation, config.group, config.orderInGroup, config.width,
                        config.displayName, config.dependents, config.recommender);
            }
        }
    }

    protected static Map<String, String> dynamicConfigUpdateModes() {
        Map<String, String> modes = new HashMap<>();
        for (String name : AllDynamicConfigs) {
            String mode = PerBrokerConfigs.contains(name) ? "per-broker" : "cluster-wide";
            modes.put(name, mode);
        }
        return modes;
    }

    protected static Properties resolveVariableConfigs(Properties propsOriginal) {
        Properties props = new Properties();
        AbstractConfig config = new AbstractConfig(new ConfigDef(), propsOriginal, false);
        for (Map.Entry<String, Object> entry : config.originals().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (!key.startsWith(AbstractConfig.CONFIG_PROVIDERS_CONFIG)) {
                props.put(key, value);
            }
        }
        return props;
    }
}
