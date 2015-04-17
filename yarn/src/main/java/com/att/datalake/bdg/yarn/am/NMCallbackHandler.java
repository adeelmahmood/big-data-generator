package com.att.datalake.bdg.yarn.am;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.springframework.stereotype.Component;

import com.att.datalake.bdg.yarn.support.AbstractCallbackHandler;

@Component
public class NMCallbackHandler extends AbstractCallbackHandler implements NMClientAsync.CallbackHandler {

	@Override
	public void onContainerStarted(ContainerId arg0, Map<String, ByteBuffer> arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onContainerStatusReceived(ContainerId arg0, ContainerStatus arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onContainerStopped(ContainerId arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onGetContainerStatusError(ContainerId arg0, Throwable arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStartContainerError(ContainerId arg0, Throwable arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStopContainerError(ContainerId arg0, Throwable arg1) {
		// TODO Auto-generated method stub
		
	}

	
}
