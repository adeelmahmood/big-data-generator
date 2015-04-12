package com.att.datalake.bdg.yarn.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.client.api.YarnClientApplication;

public interface ClientOperations {

	YarnClientApplication getNewApplication(ClientProperties properties);

	ByteBuffer obtainSecurityTokens() throws IOException;
}
