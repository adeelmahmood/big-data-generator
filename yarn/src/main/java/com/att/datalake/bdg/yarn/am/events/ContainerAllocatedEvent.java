package com.att.datalake.bdg.yarn.am.events;

import org.apache.hadoop.yarn.api.records.Container;
import org.springframework.context.ApplicationEvent;

public class ContainerAllocatedEvent extends ApplicationEvent {

	private static final long serialVersionUID = -8879706682887049236L;

	public ContainerAllocatedEvent(Container container) {
		super(container);
	}

}
