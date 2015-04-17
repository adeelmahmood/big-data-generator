package com.att.datalake.bdg.yarn.am.events;

import org.springframework.context.ApplicationEvent;

public class ShutdownRequestedEvent extends ApplicationEvent {

	private static final long serialVersionUID = -89661249112614487L;

	public ShutdownRequestedEvent() {
		super(true);
	}
}
