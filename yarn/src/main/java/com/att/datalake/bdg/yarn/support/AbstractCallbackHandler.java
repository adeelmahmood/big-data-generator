package com.att.datalake.bdg.yarn.support;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;

public abstract class AbstractCallbackHandler implements ApplicationEventPublisherAware {

	private ApplicationEventPublisher eventPublisher;

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.eventPublisher = applicationEventPublisher;
	}

	protected <T extends ApplicationEvent> void publishEvent(T event) {
		eventPublisher.publishEvent(event);
	}
}
