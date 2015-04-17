package com.att.datalake.bdg.yarn.am;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.att.datalake.bdg.yarn.am.events.ContainerAllocatedEvent;
import com.att.datalake.bdg.yarn.am.events.ContainerCompletedEvent;
import com.att.datalake.bdg.yarn.am.events.ContainerFailedEvent;
import com.att.datalake.bdg.yarn.am.events.RMErrorEvent;
import com.att.datalake.bdg.yarn.am.events.ShutdownRequestedEvent;
import com.att.datalake.bdg.yarn.support.AbstractCallbackHandler;

@Component
public class RMCallbackHandler extends AbstractCallbackHandler implements AMRMClientAsync.CallbackHandler {

	private static final Logger log = LoggerFactory.getLogger(RMCallbackHandler.class);

	@Override
	public float getProgress() {
		return 0;
	}

	@Override
	public void onContainersAllocated(List<Container> containers) {
		log.info(containers.size() + " containers allocated");
		for (Container container : containers) {
			log.info("Allocated container, containerId=" + container.getId() + ", containerNode="
					+ container.getNodeId().getHost() + ":" + container.getNodeId().getPort() + ", containerNodeURI="
					+ container.getNodeHttpAddress() + ", containerResourceMemory"
					+ container.getResource().getMemory() + ", containerResourceVirtualCores"
					+ container.getResource().getVirtualCores());
			// new container available for processing
			publishEvent(new ContainerAllocatedEvent(container));
		}
	}

	@Override
	public void onContainersCompleted(List<ContainerStatus> statuses) {
		log.info(statuses.size() + " containers responded with completed status");
		for (ContainerStatus status : statuses) {
			log.info("container status for containerId=" + status.getContainerId() + ", state=" + status.getState()
					+ ", exitStatus=" + status.getExitStatus() + ", diagnostics=" + status.getDiagnostics());
			// make sure the return state in completed
			if (status.getState() != ContainerState.COMPLETE) {
				throw new RuntimeException("unexpected container in completed status event");
			}

			// check exit status and adjust progress accordingly
			int exitStatus = status.getExitStatus();
			if (exitStatus != 0) {
				publishEvent(new ContainerFailedEvent(status));
			} else {
				// completed successfully
				publishEvent(new ContainerCompletedEvent(status));
			}
		}
	}

	@Override
	public void onError(Throwable e) {
		publishEvent(new RMErrorEvent(e));
	}

	@Override
	public void onNodesUpdated(List<NodeReport> arg0) {
	}

	@Override
	public void onShutdownRequest() {
		publishEvent(new ShutdownRequestedEvent());
	}
}
