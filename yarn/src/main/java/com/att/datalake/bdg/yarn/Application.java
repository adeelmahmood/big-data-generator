package com.att.datalake.bdg.yarn;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.att.datalake.bdg.yarn.client.ApplicationClient;

@Configuration
@EnableConfigurationProperties
@EnableAutoConfiguration
@ComponentScan
public class Application implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(Application.class);

	public static String CURRENT_JAR_PATH;

	@Bean
	public org.apache.hadoop.conf.Configuration yarnConfiguration() {
		return new YarnConfiguration();
	}

	@Bean
	public FileSystem fileSystem() throws IOException {
		return FileSystem.get(yarnConfiguration());
	}

	@Bean
	public YarnClient yarnClient() {
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(yarnConfiguration());
		return yarnClient;
	}

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Autowired
	private ApplicationClient client;

	@Override
	public void run(String... args) throws Exception {
		if (args.length == 0) {
			log.error("specify either client or application-master to run, exiting");
			System.exit(0);
		}
		log.debug("yarn application started for " + args[0]);
		setCurrentJarPath();

		if (args[0].equals("client")) {
			client.start();
		}

	}

	private void setCurrentJarPath() {
		CURRENT_JAR_PATH = new java.io.File(Application.class.getProtectionDomain().getCodeSource().getLocation()
				.getPath()).getAbsolutePath();
	}
}
