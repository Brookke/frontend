import type { Context, ContextType } from "@lit/context";
import { ContextProvider } from "@lit/context";
import type { Connection, UnsubscribeFunc } from "home-assistant-js-websocket";
import type { ReactiveElement } from "lit";

type SubscribeFn<T> = (
  connection: Connection,
  setValue: (value: T) => void
) => UnsubscribeFunc | Promise<UnsubscribeFunc>;

/**
 * A context provider that defers its data subscription until the first
 * consumer requests the context. This avoids unnecessary WebSocket
 * subscriptions for data that may never be needed.
 *
 * Consumers that request the context before data has loaded will have
 * their callbacks buffered and flushed once the first value arrives.
 */
export class LazyContextProvider<
  C extends Context<unknown, unknown>,
  T extends ContextType<C> = ContextType<C>,
> {
  private _provider: ContextProvider<C>;

  private _context: C;

  private _loaded = false;

  private _subscribing = false;

  private _connection?: Connection;

  private _unsubscribe?: UnsubscribeFunc;

  private _subscribeFn: SubscribeFn<T>;

  private _pendingCallbacks: {
    callback: (value: T, unsubscribe?: () => void) => void;
    consumerHost: Element;
    subscribe?: boolean;
  }[] = [];

  constructor(
    private _host: ReactiveElement,
    options: { context: C; subscribeFn: SubscribeFn<T> }
  ) {
    this._context = options.context;
    this._subscribeFn = options.subscribeFn;

    // Listen for context-request events BEFORE the ContextProvider does,
    // so we can intercept requests when data hasn't loaded yet.
    this._host.addEventListener(
      "context-request",
      this._onContextRequest as EventListener
    );

    // Create the underlying ContextProvider without an initial value.
    // The provider's internal value will be undefined until data loads.
    this._provider = new ContextProvider(this._host, {
      context: options.context,
    });
  }

  /**
   * Set the connection reference. Called from hassConnected().
   * Does not start subscribing -- that only happens when a consumer
   * requests the context.
   */
  setConnection(connection: Connection): void {
    this._connection = connection;

    // If we were already subscribed (reconnection scenario), re-subscribe
    if (this._loaded) {
      this._unsubscribe?.();
      this._startSubscription();
    }

    // If there were pending callbacks waiting for a connection, start now
    if (this._pendingCallbacks.length > 0 && !this._subscribing) {
      this._startSubscription();
    }
  }

  /**
   * Clean up the subscription.
   */
  unsubscribe(): void {
    if (this._unsubscribe) {
      this._unsubscribe();
      this._unsubscribe = undefined;
    }
    this._loaded = false;
    this._subscribing = false;
  }

  private _onContextRequest = (ev: Event): void => {
    const contextEvent = ev as Event & {
      context: unknown;
      callback: (value: T, unsubscribe?: () => void) => void;
      contextTarget?: Element;
      subscribe?: boolean;
    };

    // Only handle requests for our context
    if (contextEvent.context !== this._context) {
      return;
    }

    // Don't intercept if data is already loaded --
    // let the ContextProvider handle it normally
    if (this._loaded) {
      return;
    }

    const consumerHost =
      contextEvent.contextTarget ?? (ev.composedPath()[0] as Element);

    // Don't self-register
    if (consumerHost === this._host) {
      return;
    }

    // Intercept: stop propagation so the inner provider doesn't
    // call back with undefined
    ev.stopPropagation();

    // Buffer this callback
    this._pendingCallbacks.push({
      callback: contextEvent.callback,
      consumerHost,
      subscribe: contextEvent.subscribe,
    });

    // Trigger the subscription if not already in progress
    if (!this._subscribing && this._connection) {
      this._startSubscription();
    }
  };

  private _startSubscription(): void {
    if (!this._connection || this._subscribing) {
      return;
    }
    this._subscribing = true;

    const result = this._subscribeFn(this._connection, (value: T) => {
      this._loaded = true;
      this._subscribing = false;

      // Set the value on the real provider -- this updates all future consumers
      this._provider.setValue(value as ContextType<C>);

      // Flush any pending callbacks that were buffered before data loaded
      this._flushPendingCallbacks();
    });

    // Handle async unsubscribe (Promise<UnsubscribeFunc>)
    if (result instanceof Promise) {
      result.then((unsub) => {
        this._unsubscribe = unsub;
      });
    } else {
      this._unsubscribe = result;
    }
  }

  private _flushPendingCallbacks(): void {
    if (this._pendingCallbacks.length === 0) {
      return;
    }

    const pending = [...this._pendingCallbacks];
    this._pendingCallbacks = [];

    // Re-add each pending callback to the provider now that it has data
    for (const { callback, consumerHost, subscribe } of pending) {
      (
        this._provider as unknown as {
          addCallback: (
            callback: (value: T, unsubscribe?: () => void) => void,
            consumerHost: Element,
            subscribe?: boolean
          ) => void;
        }
      ).addCallback(callback, consumerHost, subscribe);
    }
  }
}
