package com.att.datalake.bdg.yarn.am.events;

import org.springframework.context.ApplicationEvent;

public class RMErrorEvent extends ApplicationEvent {

	private static final long serialVersionUID = 225911601177789974L;

	public RMErrorEvent(Throwable e) {
		super(e);
	}
}
