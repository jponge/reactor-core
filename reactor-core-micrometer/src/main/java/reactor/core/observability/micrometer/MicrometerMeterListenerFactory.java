/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.observability.micrometer;

import io.micrometer.core.instrument.MeterRegistry;

import reactor.core.observability.SignalListener;
import reactor.core.observability.SignalListenerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.concurrent.Flow;

/**
 * A {@link SignalListenerFactory} for {@link MicrometerMeterListener}.
 *
 * @author Simon Baslé
 */
class MicrometerMeterListenerFactory<T> implements SignalListenerFactory<T, MicrometerMeterListenerConfiguration> {

	final MeterRegistry registry;

	MicrometerMeterListenerFactory(MeterRegistry registry) {
		this.registry = registry;
	}

	@Override
	public MicrometerMeterListenerConfiguration initializePublisherState(Flow.Publisher<? extends T> source) {
		if (source instanceof Mono) {
			return MicrometerMeterListenerConfiguration.fromMono((Mono<?>) source, this.registry);
		}
		else if (source instanceof Flux) {
			return MicrometerMeterListenerConfiguration.fromFlux((Flux<?>) source, this.registry);
		}
		else {
			throw new IllegalArgumentException("MicrometerMeterListenerFactory must only be used via the tap operator / with a Flux or Mono");
		}
	}

	@Override
	public SignalListener<T> createListener(Flow.Publisher<? extends T> source, ContextView listenerContext,
                                            MicrometerMeterListenerConfiguration publisherContext) {
		return new MicrometerMeterListener<>(publisherContext);
	}
}
