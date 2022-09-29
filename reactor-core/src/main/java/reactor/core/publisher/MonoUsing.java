/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Uses a resource, generated by a supplier for each individual Subscriber,
 * while streaming the values from a
 * Publisher derived from the same resource and makes sure the resource is released
 * if the sequence terminates or the Subscriber cancels.
 * <p>
 * <p>
 * Eager resource cleanup happens just before the source termination and exceptions
 * raised by the cleanup Consumer may override the terminal event. Non-eager
 * cleanup will drop any exception.
 *
 * @param <T> the value type streamed
 * @param <S> the resource type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoUsing<T, S> extends Mono<T> implements Fuseable, SourceProducer<T>  {

	final Callable<S> resourceSupplier;

	final Function<? super S, ? extends Mono<? extends T>> sourceFactory;

	final Consumer<? super S> resourceCleanup;

	final boolean eager;

	MonoUsing(Callable<S> resourceSupplier,
			Function<? super S, ? extends Mono<? extends T>> sourceFactory,
			Consumer<? super S> resourceCleanup,
			boolean eager) {
		this.resourceSupplier =
				Objects.requireNonNull(resourceSupplier, "resourceSupplier");
		this.sourceFactory = Objects.requireNonNull(sourceFactory, "sourceFactory");
		this.resourceCleanup = Objects.requireNonNull(resourceCleanup, "resourceCleanup");
		this.eager = eager;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		S resource;

		try {
			resource = resourceSupplier.call();
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		Mono<? extends T> p;

		try {
			p = Objects.requireNonNull(sourceFactory.apply(resource),
					"The sourceFactory returned a null value");
		}
		catch (Throwable e) {

			try {
				resourceCleanup.accept(resource);
			}
			catch (Throwable ex) {
				e = Exceptions.addSuppressed(ex, Operators.onOperatorError(e, actual.currentContext()));
			}

			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		if (p instanceof Fuseable) {
			p.subscribe(new MonoUsingSubscriber<>(actual,
					resourceCleanup,
					resource,
					eager,
					true));
		}
		else {
			p.subscribe(new MonoUsingSubscriber<>(actual,
					resourceCleanup,
					resource,
					eager,
					false));
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	static final class MonoUsingSubscriber<T, S>
			implements InnerOperator<T, T>, QueueSubscription<T> {

		final CoreSubscriber<? super T> actual;

		final Consumer<? super S> resourceCleanup;

		final S resource;

		final boolean eager;
		final boolean allowFusion;

		Flow.Subscription s;
		@Nullable
		QueueSubscription<T> qs;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MonoUsingSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(MonoUsingSubscriber.class, "wip");

		int mode;
		boolean valued;

		MonoUsingSubscriber(CoreSubscriber<? super T> actual,
				Consumer<? super S> resourceCleanup,
				S resource,
				boolean eager,
				boolean allowFusion) {
			this.actual = actual;
			this.resourceCleanup = resourceCleanup;
			this.resource = resource;
			this.eager = eager;
			this.allowFusion = allowFusion;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED || key == Attr.CANCELLED)
				return wip == 1;
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			if (WIP.compareAndSet(this, 0, 1)) {
				s.cancel();

				cleanup();
			}
		}

		void cleanup() {
			try {
				resourceCleanup.accept(resource);
			}
			catch (Throwable e) {
				Operators.onErrorDropped(e, actual.currentContext());
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Flow.Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				if (s instanceof QueueSubscription) {
					this.qs = (QueueSubscription<T>) s;
				}

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (mode == ASYNC) {
				actual.onNext(null);
				return;
			}
			this.valued = true;

			if (eager && WIP.compareAndSet(this, 0, 1)) {
				try {
					resourceCleanup.accept(resource);
				}
				catch (Throwable e) {
					Context ctx = actual.currentContext();
					actual.onError(Operators.onOperatorError(e, ctx));
					Operators.onDiscard(t, ctx);
					return;
				}
			}

			actual.onNext(t);
			actual.onComplete();

			if (!eager && WIP.compareAndSet(this, 0, 1)) {
				try {
					resourceCleanup.accept(resource);
				}
				catch (Throwable e) {
					Operators.onErrorDropped(e, actual.currentContext());
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			if (valued && mode != ASYNC) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			if (eager && WIP.compareAndSet(this, 0, 1)) {
				try {
					resourceCleanup.accept(resource);
				}
				catch (Throwable e) {
					Throwable _e = Operators.onOperatorError(e, actual.currentContext());
					t = Exceptions.addSuppressed(_e, t);
				}
			}

			actual.onError(t);

			if (!eager && WIP.compareAndSet(this, 0, 1)) {
				cleanup();
			}
		}

		@Override
		public void onComplete() {
			if (valued && mode != ASYNC) {
				return;
			}
			//this should only happen in the empty case
			if (eager && WIP.compareAndSet(this, 0, 1)) {
				try {
					resourceCleanup.accept(resource);
				}
				catch (Throwable e) {
					actual.onError(Operators.onOperatorError(e, actual.currentContext()));
					return;
				}
			}

			actual.onComplete();

			if (!eager && WIP.compareAndSet(this, 0, 1)) {
				try {
					resourceCleanup.accept(resource);
				}
				catch (Throwable e) {
					Operators.onErrorDropped(e, actual.currentContext());
				}
			}
		}

		@Override
		public void clear() {
			if (qs != null) {
				qs.clear();
			}
		}

		@Override
		public boolean isEmpty() {
			return qs == null || qs.isEmpty();
		}

		@Override
		@Nullable
		public T poll() {
			if (mode == NONE || qs == null) {
				return null;
			}

			T v = qs.poll();

			if (v != null) {
				valued = true;
				if (eager && WIP.compareAndSet(this, 0, 1)) {
					try {
						resourceCleanup.accept(resource); //throws upwards
					}
					catch (Throwable t) {
						Operators.onDiscard(v, actual.currentContext());
						throw t;
					}
				}
			}
			else if (mode == SYNC) {
				if (!eager && WIP.compareAndSet(this, 0, 1)) {
					try {
						resourceCleanup.accept(resource);
					}
					catch (Throwable t) {
						if (!valued) throw t;
						else Operators.onErrorDropped(t, actual.currentContext());
						//returns null, ie onComplete, in the second case
					}
				}
			}
			return v;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if (qs == null) {
				mode = NONE;
				return NONE;
			}
			int m = qs.requestFusion(requestedMode);
			mode = m;
			return m;
		}

		@Override
		public int size() {
			if (qs == null) {
				return 0;
			}
			return qs.size();
		}
	}
}
