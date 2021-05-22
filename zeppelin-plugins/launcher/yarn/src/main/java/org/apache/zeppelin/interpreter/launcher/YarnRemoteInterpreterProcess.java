/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.interpreter.launcher;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


/**
 * Start interpreter in yarn container.
 */
public class YarnRemoteInterpreterProcess extends RemoteInterpreterProcess {

  private static Logger LOGGER = LoggerFactory.getLogger(YarnRemoteInterpreterProcess.class);

  private String host;
  private int port = -1;
  private ZeppelinConfiguration zConf;
  private final InterpreterLaunchContext launchContext;
  private final Properties properties;
  private final Map<String, String> envs;
  private AtomicBoolean isYarnAppRunning = new AtomicBoolean(false);
  private String errorMessage;

  /************** Hadoop related **************************/
  private Configuration hadoopConf;
  private FileSystem fs;
  private FileSystem localFs;
  private YarnClient yarnClient;
  private ApplicationId appId;
  private Path stagingDir;

  // App files are world-wide readable and owner writable -> rw-r--r--
//  private static final FsPermission APP_FILE_PERMISSION =
//          FsPermission.createImmutable(Short.parseShort("644", 8));
    private static final FsPermission APP_FILE_PERMISSION = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ_EXECUTE);

  public YarnRemoteInterpreterProcess(
          InterpreterLaunchContext launchContext,
          Properties properties,
          Map<String, String> envs,
          int connectTimeout,
          int connectionPoolSize) {
    super(connectTimeout, connectionPoolSize, launchContext.getIntpEventServerHost(), launchContext.getIntpEventServerPort());
    this.zConf = ZeppelinConfiguration.create();
    this.launchContext = launchContext;
    this.properties = properties;
    this.envs = envs;

    yarnClient = YarnClient.createYarnClient();
    this.hadoopConf = new YarnConfiguration();

    // Add core-site.xml and yarn-site.xml. This is for integration test where using MiniHadoopCluster.
    if (properties.containsKey("HADOOP_CONF_DIR") &&
            !org.apache.commons.lang3.StringUtils.isBlank(properties.getProperty("HADOOP_CONF_DIR"))) {
      File hadoopConfDir = new File(properties.getProperty("HADOOP_CONF_DIR"));
      if (hadoopConfDir.exists() && hadoopConfDir.isDirectory()) {
        File coreSite = new File(hadoopConfDir, "core-site.xml");
        try {
          this.hadoopConf.addResource(coreSite.toURI().toURL());
        } catch (MalformedURLException e) {
          LOGGER.warn("Fail to add core-site.xml: " + coreSite.getAbsolutePath(), e);
        }
        File yarnSite = new File(hadoopConfDir, "yarn-site.xml");
        try {
          this.hadoopConf.addResource(yarnSite.toURI().toURL());
        } catch (MalformedURLException e) {
          LOGGER.warn("Fail to add yarn-site.xml: " + yarnSite.getAbsolutePath(), e);
        }
      } else {
        throw new RuntimeException("HADOOP_CONF_DIR: " + hadoopConfDir.getAbsolutePath() +
                " doesn't exist or is not a directory");
      }
    }

    yarnClient.init(this.hadoopConf);
    yarnClient.start();
    try {
      this.fs = FileSystem.get(hadoopConf);
      this.localFs = FileSystem.getLocal(hadoopConf);
    } catch (IOException e) {
      throw new RuntimeException("Fail to create FileSystem", e);
    }
  }

  @Override
  public void processStarted(int port, String host) {
    this.port = port;
    this.host = host;
  }

  @Override
  public String getErrorMessage() {
    return this.errorMessage;
  }

  @Override
  public String getInterpreterGroupId() {
    return launchContext.getInterpreterGroupId();
  }

  @Override
  public String getInterpreterSettingName() {
    return launchContext.getInterpreterSettingName();
  }

  @Override
  public void start(String userName) throws IOException {
    try {
      LOGGER.info("Submitting zeppelin-interpreter app to yarn");
      final YarnClientApplication yarnApplication = yarnClient.createApplication();
      final GetNewApplicationResponse appResponse = yarnApplication.getNewApplicationResponse();
      this.appId = appResponse.getApplicationId();
      ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
      appContext = createApplicationSubmissionContext(appContext);
      yarnClient.submitApplication(appContext);

      long start = System.currentTimeMillis();
      ApplicationReport appReport = getApplicationReport(appId);
      while (appReport.getYarnApplicationState() != YarnApplicationState.FAILED &&
              appReport.getYarnApplicationState() != YarnApplicationState.FINISHED &&
              appReport.getYarnApplicationState() != YarnApplicationState.KILLED &&
              appReport.getYarnApplicationState() != YarnApplicationState.RUNNING) {
        LOGGER.info("Wait for zeppelin interpreter yarn app to be started");
        Thread.sleep(2000);
        if ((System.currentTimeMillis() - start) > getConnectTimeout()) {
          yarnClient.killApplication(this.appId);
          throw new IOException("Launching zeppelin interpreter in yarn is time out, kill it now");
        }
        appReport = getApplicationReport(appId);
      }

      if (appReport.getYarnApplicationState() != YarnApplicationState.RUNNING) {
        this.errorMessage = appReport.getDiagnostics();
        throw new Exception("Failed to submit application to YARN"
                + ", applicationId=" + appId
                + ", diagnostics=" + appReport.getDiagnostics());
      }
      isYarnAppRunning.set(true);
    } catch (Exception e) {
      LOGGER.error("Fail to launch yarn interpreter process", e);
      throw new IOException(e);
    }
   finally {
     if (stagingDir != null) {
       this.fs.delete(stagingDir, true);
     }
   }
  }

  private ApplicationReport getApplicationReport(ApplicationId appId) throws YarnException, IOException {
    ApplicationReport report = yarnClient.getApplicationReport(appId);
    if (report.getYarnApplicationState() == null) {
      // The state can be null when the ResourceManager does not know about the app but the YARN
      // application history server has an incomplete entry for it. Treat this scenario as if the
      // application does not exist, since the final app status cannot be determined. This also
      // matches the behavior for this scenario if the history server was not configured.
      throw new ApplicationNotFoundException("YARN reports no state for application "
              + appId);
    }
    return report;
  }

  private ApplicationSubmissionContext createApplicationSubmissionContext(
          ApplicationSubmissionContext appContext) throws Exception {

    setResources(appContext);
    setPriority(appContext);
    setQueue(appContext);
    appContext.setApplicationId(appId);
    setApplicationName(appContext);
    appContext.setApplicationType("ZEPPELIN INTERPRETER");
    appContext.setMaxAppAttempts(1);

    ContainerLaunchContext amContainer = setUpAMLaunchContext();
    appContext.setAMContainerSpec(amContainer);
    appContext.setCancelTokensWhenComplete(true);
    return appContext;
  }


  private Path getApplicationDirPath(final Path homeDir, final ApplicationId applicationId) {
    return new Path(homeDir , ".zeppelinStaging/" + applicationId+ '/');
//    return new Path(fs.getHomeDirectory() , "/.zeppelinStaging_"+appId.toString());
  }

  private Path getApplicationDir(final ApplicationId applicationId) throws IOException {

    final Path applicationDir = getApplicationDirPath(fs.getHomeDirectory() , applicationId);
    if (!fs.exists(applicationDir)) {
      fs.mkdirs(applicationDir, APP_FILE_PERMISSION);
      fs.makeQualified(applicationDir);
      fs.setPermission(applicationDir, APP_FILE_PERMISSION);
    }
    return applicationDir;
  }

  /**
   * 复制hdfs文件到本地目录
   * @param remotePath hdfs文件
   * @param localPath 本地目录
   * @throws IOException 异常
   */
  public void copyFileToLocal(Path remotePath,Path localPath) throws IOException {
    LOGGER.debug("拷贝"+remotePath.toUri()+"到"+localPath.toUri()+"...");
    boolean ok = FileUtil.copy(remotePath.getFileSystem(hadoopConf),
            remotePath,
            localPath.getFileSystem(hadoopConf),
            localPath, false, hadoopConf);
    if(!ok)
      LOGGER.warn("拷贝"+remotePath.toUri()+"到"+localPath.toUri()+"失败！");
  }


  private ContainerLaunchContext setUpAMLaunchContext() throws IOException {
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    // Set the resources to localize
    //set stagingdir
    this.stagingDir = getApplicationDir(appId);

    LOGGER.info("default directory: "+fs.getHomeDirectory()+", working directory: "+fs.getWorkingDirectory()+", app dist directory:"+this.stagingDir.toUri());
    LOGGER.info("localFS directory: "+localFs.getHomeDirectory()+", working directory: "+localFs.getWorkingDirectory());


    Map<String, LocalResource> localResources = new HashMap<>();
    File interpreterZip = createInterpreterZip();
    Path srcPath = new Path(interpreterZip.toURI());
    srcPath = srcPath.makeQualified(localFs.getUri(),localFs.getWorkingDirectory());
    Path destPath = copyFileToRemote(stagingDir, srcPath, (short) 1);
    addResource(fs, destPath, localResources, LocalResourceType.ARCHIVE, "zeppelin");
//    FileUtils.forceDelete(interpreterZip);

    // TODO(zjffdu) Should not add interpreter specific logic here.
    if (launchContext.getInterpreterSettingGroup().equals("flink")) {
      File flinkZip = createFlinkZip();
      srcPath = localFs.makeQualified(new Path(flinkZip.toURI()));
      srcPath = srcPath.makeQualified(localFs.getUri(),localFs.getWorkingDirectory());
      destPath = copyFileToRemote(stagingDir, srcPath, (short) 1);
      addResource(fs, destPath, localResources, LocalResourceType.ARCHIVE, "flink");
//      FileUtils.forceDelete(flinkZip);

      String hdfsHiveConfDir = launchContext.getProperties().getProperty("HDFS_HIVE_CONF_DIR");
      String hiveConfDir = launchContext.getProperties().getProperty("HIVE_CONF_DIR");
      FileSystem hdfs ;
      if (!org.apache.commons.lang3.StringUtils.isBlank(hdfsHiveConfDir)
//              &&
//              !org.apache.commons.lang3.StringUtils.isBlank(defaultFs)
          ) {
       LOGGER.warn("您配置了HDFS_HIVE_CONF_DIR，此配置会覆盖HIVE_CONF_DIR指定的目录！");
       File localHiveConfFile = new File(hiveConfDir+File.separator);
       if(!localHiveConfFile.exists()) localHiveConfFile.mkdir();
       //hdfs配置文件目录
        Path hdfsHiveConfPath = new Path(hdfsHiveConfDir);
        hdfsHiveConfPath.makeQualified(hdfsHiveConfPath.toUri(),fs.getWorkingDirectory());
        FileSystem remoteFS = hdfsHiveConfPath.getFileSystem(hadoopConf);
        LOGGER.info("HDFS集群路径："+remoteFS.getUri());
        LOGGER.info("HIVE HDFS 配置目录："+hdfsHiveConfPath.toUri());
        //本地配置文件目录
        Path localHiveConfPath = localFs.makeQualified(new Path(localHiveConfFile.toURI()));
        localHiveConfPath = localHiveConfPath.makeQualified(localFs.getUri(),localFs.getWorkingDirectory());
        //递归遍历 拷贝配置文件到本地
        if(remoteFS.isDirectory(hdfsHiveConfPath)){
          FileStatus [] subFiles = remoteFS.listStatus(hdfsHiveConfPath) ;

          for(FileStatus subFile : subFiles){
            LOGGER.info("正在拷贝HIVE配置文件：源路径："+subFile.getPath().toUri()+"，目标路径："+localHiveConfPath.toUri()+"。");
            copyFileToLocal(subFile.getPath(),localHiveConfPath);
          }
        }else{
          copyFileToLocal(hdfsHiveConfPath,localHiveConfPath);
        }
      }



      if (!org.apache.commons.lang3.StringUtils.isBlank(hiveConfDir)) {
        File hiveConfZipFile = createHiveConfZip(new File(hiveConfDir));
        srcPath = localFs.makeQualified(new Path(hiveConfZipFile.toURI()));
        destPath = copyFileToRemote(stagingDir, srcPath, (short) 1);
        addResource(fs, destPath, localResources, LocalResourceType.ARCHIVE, "hive_conf");
      }

    }
    amContainer.setLocalResources(localResources);

    // Setup the command to run the AM
    List<String> vargs = new ArrayList<>();
    vargs.add(ApplicationConstants.Environment.PWD.$() + "/zeppelin/bin/interpreter.sh");
    vargs.add("-d");
    vargs.add(ApplicationConstants.Environment.PWD.$() + "/zeppelin/interpreter/"
            + launchContext.getInterpreterSettingGroup());
    vargs.add("-c");
    vargs.add(launchContext.getIntpEventServerHost());
    vargs.add("-p");
    vargs.add(launchContext.getIntpEventServerPort() + "");
    vargs.add("-r");
    vargs.add(zConf.getInterpreterPortRange() + "");
    vargs.add("-i");
    vargs.add(launchContext.getInterpreterGroupId());
    vargs.add("-l");
    vargs.add(ApplicationConstants.Environment.PWD.$() + "/zeppelin/" +
            ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_LOCALREPO.getStringValue()
            + "/" + launchContext.getInterpreterSettingName());
    vargs.add("-g");
    vargs.add(launchContext.getInterpreterSettingName());

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
            File.separator + ApplicationConstants.STDOUT);
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
            File.separator + ApplicationConstants.STDERR);

    // Setup ContainerLaunchContext for AM container
    amContainer.setCommands(vargs);

    // pass the interpreter ENV to yarn container and also add hadoop jars to CLASSPATH
    populateHadoopClasspath(this.envs);
    if (this.launchContext.getInterpreterSettingGroup().equals("flink")) {
      // Update the flink related env because the all these are different in yarn container
      this.envs.put("FLINK_HOME", ApplicationConstants.Environment.PWD.$() + "/flink");
      this.envs.put("FLINK_CONF_DIR", ApplicationConstants.Environment.PWD.$() + "/flink/conf");
      this.envs.put("FLINK_LIB_DIR", ApplicationConstants.Environment.PWD.$() + "/flink/lib");
      this.envs.put("FLINK_PLUGINS_DIR", ApplicationConstants.Environment.PWD.$() + "/flink/plugins");
      this.envs.put("HIVE_CONF_DIR", ApplicationConstants.Environment.PWD.$() + "/hive_conf");
    }
    // set -Xmx
    int memory = Integer.parseInt(
            properties.getProperty("zeppelin.interpreter.yarn.resource.memory", "1024"));
    this.envs.put("ZEPPELIN_INTP_MEM", "-Xmx" + memory + "m");
    amContainer.setEnvironment(this.envs);

    return amContainer;
  }

  /**
   * Populate the classpath entry in the given environment map with any application
   * classpath specified through the Hadoop and Yarn configurations.
   */
  private void populateHadoopClasspath(Map<String, String> envs) {
    List<String> yarnClassPath = Lists.newArrayList(getYarnAppClasspath());
    List<String> mrClassPath = Lists.newArrayList(getMRAppClasspath());
    yarnClassPath.addAll(mrClassPath);
    LOGGER.info("Adding hadoop classpath: " + org.apache.commons.lang3.StringUtils.join(yarnClassPath, ":"));
    for (String path : yarnClassPath) {
      String newValue = path;
      if (envs.containsKey(ApplicationConstants.Environment.CLASSPATH.name())) {
        newValue = envs.get(ApplicationConstants.Environment.CLASSPATH.name()) +
                ApplicationConstants.CLASS_PATH_SEPARATOR + newValue;
      }
      envs.put(ApplicationConstants.Environment.CLASSPATH.name(), newValue);
    }
    // set HADOOP_MAPRED_HOME explicitly, otherwise it won't work for hadoop3
    // see https://stackoverflow.com/questions/50719585/unable-to-run-mapreduce-wordcount
    this.envs.put("HADOOP_MAPRED_HOME", "${HADOOP_HOME}");
  }

  private String[] getYarnAppClasspath() {
    String[] classpaths = hadoopConf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
    if (classpaths == null || classpaths.length == 0) {
      return getDefaultYarnApplicationClasspath();
    } else {
      return classpaths;
    }
  }

  private String[] getMRAppClasspath() {
    String[] classpaths = hadoopConf.getStrings("mapreduce.application.classpath");
    if (classpaths == null || classpaths.length == 0) {
      return getDefaultMRApplicationClasspath();
    } else {
      return classpaths;
    }
  }

  private String[] getDefaultYarnApplicationClasspath() {
    return YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH;
  }

  private String[] getDefaultMRApplicationClasspath() {
    return StringUtils.getStrings(MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH);
  }

  private void setResources(ApplicationSubmissionContext appContext) {
    int memory = Integer.parseInt(
            properties.getProperty("zeppelin.interpreter.yarn.resource.memory", "1024"));
    int memoryOverHead = Integer.parseInt(
            properties.getProperty("zeppelin.interpreter.yarn.resource.memoryOverhead", "384"));
    if (memoryOverHead < memory * 0.1) {
      memoryOverHead = 384;
    }
    int cores = Integer.parseInt(
            properties.getProperty("zeppelin.interpreter.yarn.resource.cores", "1"));
    final Resource resource = Resource.newInstance(memory + memoryOverHead, cores);
    appContext.setResource(resource);
  }

  private void setPriority(ApplicationSubmissionContext appContext) {
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(1);
    appContext.setPriority(pri);
  }

  private void setQueue(ApplicationSubmissionContext appContext) {
    String queue = properties.getProperty("zeppelin.interpreter.yarn.queue", "default");
    appContext.setQueue(queue);
  }

  private void setApplicationName(ApplicationSubmissionContext appContext) {
    appContext.setApplicationName("Zeppelin Interpreter " + launchContext.getInterpreterGroupId());
  }

  /**
   * @param zos
   * @param srcFile
   * @param parentDirectoryName
   * @throws IOException
   */
  private void addFileToZipStream(ZipOutputStream zos,
                                  File srcFile,
                                  String parentDirectoryName) throws IOException {
    if (srcFile == null || !srcFile.exists()) {
      return;
    }

    String zipEntryName = srcFile.getName();
    if (parentDirectoryName != null && !parentDirectoryName.isEmpty()) {
      zipEntryName = parentDirectoryName + "/" + srcFile.getName();
    }

    if (srcFile.isDirectory()) {
      for (File file : srcFile.listFiles()) {
        addFileToZipStream(zos, file, zipEntryName);
      }
    } else {
      zos.putNextEntry(new ZipEntry(zipEntryName));
      Files.copy(srcFile, zos);
      zos.closeEntry();
    }
  }



  /**
   *
   * Create zip file to interpreter.
   * The contents are all the stuff under ZEPPELIN_HOME/interpreter/{interpreter_name}
   * @return
   * @throws IOException
   */
  private File createInterpreterZip() throws IOException {
    File interpreterArchive = new File("/tmp/zeppelin_interpreter.zip");
    if(interpreterArchive!=null && interpreterArchive.exists()){
      interpreterArchive.delete();
    }
    interpreterArchive.createNewFile();
    LOGGER.info("create zeppelin interpeter dir:"+interpreterArchive.getAbsolutePath());

    ZipOutputStream interpreterZipStream = new ZipOutputStream(new FileOutputStream(interpreterArchive));
    interpreterZipStream.setLevel(0);

    String zeppelinHomeEnv = System.getenv("ZEPPELIN_HOME");
    LOGGER.info("zeppelin_home:"+zeppelinHomeEnv);
    if (org.apache.commons.lang3.StringUtils.isBlank(zeppelinHomeEnv)) {
      throw new IOException("ZEPPELIN_HOME is not specified");
    }
    File zeppelinHome = new File(zeppelinHomeEnv);
    File binDir = new File(zeppelinHome, "bin");
    addFileToZipStream(interpreterZipStream, binDir, null);
    LOGGER.info("拷贝bin目录到zip包！");
    File confDir = new File(zeppelinHome, "conf");
    addFileToZipStream(interpreterZipStream, confDir, null);

    File interpreterDir = new File(zeppelinHome, "interpreter/" + launchContext.getInterpreterSettingGroup());
    addFileToZipStream(interpreterZipStream, interpreterDir, "interpreter");

    File localRepoDir = new File(zConf.getInterpreterLocalRepoPath() + "/"
            + launchContext.getInterpreterSettingName());
    if (localRepoDir.exists() && localRepoDir.isDirectory()) {
      LOGGER.debug("Adding localRepoDir {} to interpreter zip: ", localRepoDir.getAbsolutePath());
      addFileToZipStream(interpreterZipStream, localRepoDir, "local-repo");
    }

    // add zeppelin-interpreter-shaded jar
    File[] interpreterShadedFiles = new File(zeppelinHome, "interpreter").listFiles(
            file -> file.getName().startsWith("zeppelin-interpreter-shaded")
                    && file.getName().endsWith(".jar"));
    if (interpreterShadedFiles.length == 0) {
      throw new IOException("No zeppelin-interpreter-shaded jar found under " +
              zeppelinHome.getAbsolutePath() + "/interpreter");
    }
    if (interpreterShadedFiles.length > 1) {
      throw new IOException("More than 1 zeppelin-interpreter-shaded jars found under "
              + zeppelinHome.getAbsolutePath() + "/interpreter");
    }
    addFileToZipStream(interpreterZipStream, interpreterShadedFiles[0], "interpreter");

    interpreterZipStream.flush();
    interpreterZipStream.close();
    return interpreterArchive;
  }

  private File createFlinkZip() throws IOException {
    File flinkArchive = new File("/tmp/zeppelin_flink.zip");
    if(flinkArchive!=null && flinkArchive.exists()){
      flinkArchive.delete();
    }
    flinkArchive.createNewFile();

    ZipOutputStream flinkZipStream = new ZipOutputStream(new FileOutputStream(flinkArchive));
    flinkZipStream.setLevel(0);

    String flinkHomeEnv = envs.get("FLINK_HOME");
    File flinkHome = new File(flinkHomeEnv);
    if (!flinkHome.exists() || !flinkHome.isDirectory()) {
      throw new IOException("FLINK_HOME " + flinkHome.getAbsolutePath() +
              " doesn't exist or is not a directory.");
    }
    for (File file : flinkHome.listFiles()) {
      addFileToZipStream(flinkZipStream, file, null);
    }

    flinkZipStream.flush();
    flinkZipStream.close();
    return flinkArchive;
  }



  private File createHiveConfZip(File hiveConfDir) throws IOException {
    File hiveConfArchive = new File("/tmp/zeppelin_hive_conf.zip");
    if(hiveConfArchive!=null && hiveConfArchive.exists()){
      hiveConfArchive.delete();
    }
    hiveConfArchive.createNewFile();

    ZipOutputStream hiveConfZipStream = new ZipOutputStream(new FileOutputStream(hiveConfArchive));
    hiveConfZipStream.setLevel(0);

    Path hiveHdfsPath = new Path("HIVE_CONF_DIR ");
    if (!hiveConfDir.exists()) {
        throw new IOException("HIVE_CONF_DIR " + hiveConfDir.getAbsolutePath() + " doesn't exist");
    }


    for (File file : hiveConfDir.listFiles()) {
      addFileToZipStream(hiveConfZipStream, file, null);
    }

    hiveConfZipStream.flush();
    hiveConfZipStream.close();
    return hiveConfArchive;
  }

  /**
   *
   * @param destDir 目标路径
   * @param srcPath 源路径
   * @param replication 副本数
   * @return
   * @throws IOException
   */
  private Path copyFileToRemote(  Path destDir,
          Path srcPath,
          Short replication) throws IOException {
    LOGGER.info("文件拷贝：参数：destdir:"+destDir+",srcPath:"+srcPath+",replication:"+replication);
    FileSystem destFs = destDir.getFileSystem(hadoopConf);
    FileSystem srcFs = srcPath.getFileSystem(hadoopConf);
    Path destPath = new Path(destDir, srcPath.getName());
    LOGGER.info("目标HDFS路径："+destPath.toUri());
    LOGGER.info("源头HDFS路径："+srcPath.toUri());
    LOGGER.info("Uploading resource " + srcPath + " to " + destPath);
    try {
      boolean ok = FileUtil.copy(srcFs, srcPath, destFs, destPath, false, hadoopConf);
      if(ok){
        LOGGER.info("文件上传成功！！！");
      }
    }catch (IOException ex){
      LOGGER.error("数据上传到hdfs失败！",ex);
      throw  ex;
    }
    destFs.setReplication(destPath, replication);
    destFs.setPermission(destPath, APP_FILE_PERMISSION);

    return destPath;
  }

  private void addResource(
          FileSystem fs,
          Path destPath,
          Map<String, LocalResource> localResources,
          LocalResourceType resourceType,
          String link) throws IOException {

    FileStatus destStatus = fs.getFileStatus(destPath);
    LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
    amJarRsrc.setType(resourceType);
    amJarRsrc.setVisibility(LocalResourceVisibility.PUBLIC);
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(destPath));
    amJarRsrc.setTimestamp(destStatus.getModificationTime());
    amJarRsrc.setSize(destStatus.getLen());
    localResources.put(link, amJarRsrc);
  }

  @Override
  public void stop() {
    if (isRunning()) {
      LOGGER.info("Kill interpreter process");
      try {
        callRemoteFunction(client -> {
          client.shutdown();
          return null;
        });
      } catch (Exception e) {
        LOGGER.warn("ignore the exception when shutting down", e);
      }

      // Shutdown connection
      shutdown();
    }

    yarnClient.stop();
    LOGGER.info("Remote process terminated");
  }

  @Override
  public String getHost() {
    return this.host;
  }

  @Override
  public int getPort() {
    return this.port;
  }

  @Override
  public boolean isRunning() {
    return isYarnAppRunning.get();
  }
}
