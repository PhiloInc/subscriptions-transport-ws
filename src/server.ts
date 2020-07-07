import * as WebSocket from 'ws';

import MessageTypes from './message-types';
import { GRAPHQL_WS, GRAPHQL_SUBSCRIPTIONS } from './protocol';
import isObject from './utils/is-object';
import {
  parse,
  ExecutionResult,
  GraphQLSchema,
  DocumentNode,
  validate,
  ValidationContext,
  specifiedRules,
  GraphQLFieldResolver,
} from 'graphql';
import { createEmptyIterable } from './utils/empty-iterable';
import { createAsyncIterator, forAwaitEach, isAsyncIterable } from 'iterall';
import { isASubscriptionOperation } from './utils/is-subscriptions';
import { parseLegacyProtocolMessage } from './legacy/parse-legacy-protocol';
import { IncomingMessage } from 'http';
import isString from './utils/is-string';

export type ExecutionIterator = AsyncIterator<ExecutionResult>;

export interface ExecutionParams<TContext = any> {
  query: string | DocumentNode;
  variables: { [key: string]: any };
  operationName: string | undefined;
  context: TContext;
  formatResponse?: (executionResult: ExecutionResult, executionParams: ExecutionParams<TContext>) => ExecutionResult;
  formatError?: (error: Error, executionParams: ExecutionParams<TContext>) => Error;
  schema?: GraphQLSchema;
}

export type InitPromiseResult<TContext = any> = boolean | TContext;

export type ConnectionContext = {
  initPromise: Promise<InitPromiseResult>,
  isLegacy: boolean,
  socket: WebSocket,
  request: IncomingMessage,
  operations: {
    [opId: string]: ExecutionIterator,
  },
};

export interface OperationMessagePayload {
  [key: string]: any; // this will support for example any options sent in init like the auth token
  query?: string;
  variables?: { [key: string]: any };
  operationName?: string;
}

export interface OperationMessage {
  payload?: OperationMessagePayload;
  id?: string;
  type: string;
}

export type ExecuteFunction = (schema: GraphQLSchema,
                               document: DocumentNode,
                               rootValue?: any,
                               contextValue?: any,
                               variableValues?: { [key: string]: any },
                               operationName?: string,
                               fieldResolver?: GraphQLFieldResolver<any, any>) =>
                               ExecutionResult |
                               Promise<ExecutionResult> |
                               AsyncIterator<ExecutionResult>;

export type SubscribeFunction = (schema: GraphQLSchema,
                                 document: DocumentNode,
                                 rootValue?: any,
                                 contextValue?: any,
                                 variableValues?: { [key: string]: any },
                                 operationName?: string,
                                 fieldResolver?: GraphQLFieldResolver<any, any>,
                                 subscribeFieldResolver?: GraphQLFieldResolver<any, any>) =>
                                 AsyncIterator<ExecutionResult> |
                                 Promise<AsyncIterator<ExecutionResult> | ExecutionResult>;

export type OnDisconnectFunction = (webSocket: WebSocket, connectionContext: ConnectionContext) => void;
export type OnConnectFunction = (
  payload: OperationMessagePayload | undefined,
  webSocket: WebSocket,
  connectionContext: ConnectionContext) => InitPromiseResult | Promise<InitPromiseResult>;
export type OnOperationFunction = (
  message: OperationMessage,
  executionParams: ExecutionParams,
  webSocket: WebSocket) => ExecutionParams | Promise<ExecutionParams>;
export type OnOperationCompleteFunction = (webSocket: WebSocket, opId: string | undefined) => void;

export interface ServerOptions {
  rootValue?: any;
  schema?: GraphQLSchema;
  execute?: ExecuteFunction;
  subscribe?: SubscribeFunction;
  validationRules?:
    Array<(context: ValidationContext) => any> | ReadonlyArray<any>;
  onOperation?: OnOperationFunction;
  onOperationComplete?: OnOperationCompleteFunction;
  onConnect?: OnConnectFunction;
  onDisconnect?: OnDisconnectFunction;
  keepAlive?: number;
}

const isWebSocketServer = (socket: any) => socket.on;

export class SubscriptionServer {
  private onOperation: OnOperationFunction | undefined;
  private onOperationComplete: OnOperationCompleteFunction | undefined;
  private onConnect: OnConnectFunction | undefined;
  private onDisconnect: OnDisconnectFunction | undefined;

  private wsServer: WebSocket.Server;
  private execute: ExecuteFunction;
  private subscribe: SubscribeFunction | undefined;
  private schema: GraphQLSchema | undefined;
  private rootValue: any;
  private keepAlive: number | undefined;
  private closeHandler: () => void;
  private specifiedRules:
    Array<(context: ValidationContext) => any> |
    ReadonlyArray<any>;

  public static create(options: ServerOptions, socketOptionsOrServer: WebSocket.ServerOptions | WebSocket.Server) {
    return new SubscriptionServer(options, socketOptionsOrServer);
  }

  constructor(options: ServerOptions, socketOptionsOrServer: WebSocket.ServerOptions | WebSocket.Server) {
    const {
      onOperation, onOperationComplete, onConnect, onDisconnect, keepAlive,
    } = options;

    this.specifiedRules = options.validationRules || specifiedRules;
    this.loadExecutor(options);

    this.onOperation = onOperation;
    this.onOperationComplete = onOperationComplete;
    this.onConnect = onConnect;
    this.onDisconnect = onDisconnect;
    this.keepAlive = keepAlive;

    if (isWebSocketServer(socketOptionsOrServer)) {
      this.wsServer = <WebSocket.Server>socketOptionsOrServer;
    } else {
      // Init and connect WebSocket server to http
      this.wsServer = new WebSocket.Server(socketOptionsOrServer || {});
    }

    const connectionHandler = ((socket: WebSocket, request: IncomingMessage) => {
      // Add `upgradeReq` to the socket object to support old API, without creating a memory leak
      // See: https://github.com/websockets/ws/pull/1099
      (socket as any).upgradeReq = request;
      // NOTE: the old GRAPHQL_SUBSCRIPTIONS protocol support should be removed in the future
      if (socket.protocol === undefined ||
        (socket.protocol.indexOf(GRAPHQL_WS) === -1 && socket.protocol.indexOf(GRAPHQL_SUBSCRIPTIONS) === -1)) {
        // Close the connection with an error code, ws v2 ensures that the
        // connection is cleaned up even when the closing handshake fails.
        // 1002: protocol error
        socket.close(1002);

        return;
      }

      const connectionContext: ConnectionContext = {
        initPromise: Promise.resolve(true),
        isLegacy: false,
        socket: socket,
        request: request,
        operations: {},
      };

      const connectionClosedHandler = (error: any) => {
        if (error) {
          this.sendError(
            connectionContext,
            '',
            { message: error.message ? error.message : error },
            MessageTypes.GQL_CONNECTION_ERROR,
          );

          setTimeout(() => {
            // 1011 is an unexpected condition prevented the request from being fulfilled
            connectionContext.socket.close(1011);
          }, 10);
        }
        this.onClose(connectionContext);

        if (this.onDisconnect) {
          this.onDisconnect(socket, connectionContext);
        }
      };

      socket.on('error', connectionClosedHandler);
      socket.on('close', connectionClosedHandler);
      socket.on('message', this.onMessage(connectionContext));
    });

    this.wsServer.on('connection', connectionHandler);
    this.closeHandler = () => {
      this.wsServer.removeListener('connection', connectionHandler);
      this.wsServer.close();
    };
  }

  public get server(): WebSocket.Server {
    return this.wsServer;
  }

  public close(): void {
    this.closeHandler();
  }

  private loadExecutor(options: ServerOptions) {
    const { execute, subscribe, schema, rootValue } = options;

    if (!execute) {
      throw new Error('Must provide `execute` for websocket server constructor.');
    }

    this.schema = schema;
    this.rootValue = rootValue;
    this.execute = execute;
    this.subscribe = subscribe;
  }

  private unsubscribe(connectionContext: ConnectionContext, opId: string) {
    const operation = connectionContext.operations[opId];
    if (operation) {
      if (operation.return) {
        operation.return();
      }

      delete connectionContext.operations[opId];

      if (this.onOperationComplete) {
        this.onOperationComplete(connectionContext.socket, opId);
      }
    }
  }

  private onClose(connectionContext: ConnectionContext) {
    Object.keys(connectionContext.operations).forEach((opId) => {
      this.unsubscribe(connectionContext, opId);
    });
  }

  private onMessage(connectionContext: ConnectionContext) {
    return (message: any) => {
      let parsedMessage: OperationMessage;
      try {
        parsedMessage = parseLegacyProtocolMessage(connectionContext, JSON.parse(message));
      } catch (e) {
        this.sendError(connectionContext, undefined, { message: e.message }, MessageTypes.GQL_CONNECTION_ERROR);
        return;
      }

      const { id: opId, payload, type } = parsedMessage;
      switch (type) {
        case MessageTypes.GQL_CONNECTION_INIT:
          const onConnect = this.onConnect;
          if (onConnect) {
            connectionContext.initPromise = new Promise((resolve, reject) => {
              try {
                // TODO - this should become a function call with just 2 arguments in the future
                // when we release the breaking change api: parsedMessage.payload and connectionContext
                resolve(onConnect(payload, connectionContext.socket, connectionContext));
              } catch (e) {
                reject(e);
              }
            });
          }

          connectionContext.initPromise.then((result) => {
            if (result === false) {
              throw new Error('Prohibited connection!');
            }

            this.sendMessage(
              connectionContext,
              undefined,
              MessageTypes.GQL_CONNECTION_ACK,
              undefined,
            );

            if (this.keepAlive) {
              this.sendKeepAlive(connectionContext);
              // Regular keep alive messages if keepAlive is set
              const keepAliveTimer = setInterval(() => {
                if (connectionContext.socket.readyState === WebSocket.OPEN) {
                  this.sendKeepAlive(connectionContext);
                } else {
                  clearInterval(keepAliveTimer);
                }
              }, this.keepAlive);
            }
          }).catch((error: Error) => {
            this.sendError(
              connectionContext,
              opId,
              { message: error.message },
              MessageTypes.GQL_CONNECTION_ERROR,
            );

            // Close the connection with an error code, ws v2 ensures that the
            // connection is cleaned up even when the closing handshake fails.
            // 1011: an unexpected condition prevented the operation from being fulfilled
            // We are using setTimeout because we want the message to be flushed before
            // disconnecting the client
            setTimeout(() => {
              connectionContext.socket.close(1011);
            }, 10);
          });
          break;

        case MessageTypes.GQL_CONNECTION_TERMINATE:
          connectionContext.socket.close();
          break;

        case MessageTypes.GQL_START:
          if (!isString(opId)) {
            this.sendError(connectionContext, opId, { message: 'opId missing' });
            return;
          }
          if (!payload) {
            this.sendError(connectionContext, opId, { message: 'payload missing' });
            return;
          }
          const { query } = payload;
          if (!query) {
            this.sendError(connectionContext, opId, { message: 'query missing' });
            return;
          }
          connectionContext.initPromise.then((initResult) => {
            // if we already have a subscription with this id, unsubscribe from it first
            if (connectionContext.operations && connectionContext.operations[opId]) {
              this.unsubscribe(connectionContext, opId);
            }

            const baseParams: ExecutionParams = {
              query,
              variables: payload.variables || {},
              operationName: payload.operationName,
              context: isObject(initResult) ? Object.assign(Object.create(Object.getPrototypeOf(initResult)), initResult) : {},
              schema: this.schema,
            };
            let promisedParams = Promise.resolve(baseParams);

            // set an initial mock subscription to only registering opId
            connectionContext.operations[opId] = createEmptyIterable();

            if (this.onOperation) {
              promisedParams = Promise.resolve(this.onOperation(parsedMessage, baseParams, connectionContext.socket));
            }

            promisedParams.then((params) => {
              if (!isObject(params)) {
                const error = `Invalid params returned from onOperation! return values must be an object!`;
                this.sendError(connectionContext, opId, { message: error });

                throw new Error(error);
              }

              if (!params.schema) {
                const error = 'Missing schema information. The GraphQL schema should be provided either statically in' +
                  ' the `SubscriptionServer` constructor or as a property on the object returned from onOperation!';
                this.sendError(connectionContext, opId, { message: error });

                throw new Error(error);
              }

              const document = isString(params.query) ?  parse(params.query) : params.query;
              let executionPromise: Promise<AsyncIterator<ExecutionResult> | ExecutionResult>;
              const validationErrors = validate(params.schema, document, this.specifiedRules);

              if ( validationErrors.length > 0 ) {
                executionPromise = Promise.resolve({ errors: validationErrors });
              } else {
                let executor: SubscribeFunction | ExecuteFunction = this.execute;
                if (this.subscribe && isASubscriptionOperation(document, params.operationName)) {
                  executor = this.subscribe;
                }
                executionPromise = Promise.resolve(executor(params.schema,
                  document,
                  this.rootValue,
                  params.context,
                  params.variables,
                  params.operationName));
              }

              return executionPromise.then((executionResult) => ({
                executionIterable: isAsyncIterable(executionResult) ?
                  executionResult : createAsyncIterator([ executionResult ]),
                params,
              }));
            }).then(({ executionIterable, params }) => {
              forAwaitEach(
                executionIterable as any,
                (value: ExecutionResult) => {
                  let result = value;

                  if (params.formatResponse) {
                    try {
                      result = params.formatResponse(value, params);
                    } catch (err) {
                      console.error('Error in formatError function:', err);
                    }
                  }

                  this.sendMessage(connectionContext, opId, MessageTypes.GQL_DATA, result);
                })
                .then(() => {
                  this.sendMessage(connectionContext, opId, MessageTypes.GQL_COMPLETE, null);
                })
                .catch((e: Error) => {
                  let error = e;

                  if (params.formatError) {
                    try {
                      error = params.formatError(e, params);
                    } catch (err) {
                      console.error('Error in formatError function: ', err);
                    }
                  }

                  // plain Error object cannot be JSON stringified.
                  if (Object.keys(e).length === 0) {
                    error = { name: e.name, message: e.message };
                  }

                  this.sendError(connectionContext, opId, error);
                });

              return executionIterable;
            }).then((subscription: ExecutionIterator) => {
              connectionContext.operations[opId] = subscription;
            }).then(() => {
              // NOTE: This is a temporary code to support the legacy protocol.
              // As soon as the old protocol has been removed, this coode should also be removed.
              this.sendMessage(connectionContext, opId, MessageTypes.SUBSCRIPTION_SUCCESS, undefined);
            }).catch((e: any) => {
              if (e.errors) {
                this.sendMessage(connectionContext, opId, MessageTypes.GQL_DATA, { errors: e.errors });
              } else {
                this.sendError(connectionContext, opId, { message: e.message });
              }

              // Remove the operation on the server side as it will be removed also in the client
              this.unsubscribe(connectionContext, opId);
              return;
            });
            return promisedParams;
          }).catch((error) => {
            // Handle initPromise rejected
            this.sendError(connectionContext, opId, { message: error.message });
            this.unsubscribe(connectionContext, opId);
          });
          break;

        case MessageTypes.GQL_STOP:
          // Find subscription id. Call unsubscribe.
          if (!isString(opId)) {
            this.sendError(connectionContext, opId, { message: 'opId missing' });
            return;
          }
          this.unsubscribe(connectionContext, opId);
          break;

        default:
          this.sendError(connectionContext, opId, { message: 'Invalid message type!' });
      }
    };
  }

  private sendKeepAlive(connectionContext: ConnectionContext): void {
    if (connectionContext.isLegacy) {
      this.sendMessage(connectionContext, undefined, MessageTypes.KEEP_ALIVE, undefined);
    } else {
      this.sendMessage(connectionContext, undefined, MessageTypes.GQL_CONNECTION_KEEP_ALIVE, undefined);
    }
  }

  private sendMessage(connectionContext: ConnectionContext, opId: string | undefined, type: string, payload: any): void {
    const parsedMessage = parseLegacyProtocolMessage(connectionContext, {
      type,
      id: opId,
      payload,
    });

    if (parsedMessage && connectionContext.socket.readyState === WebSocket.OPEN) {
      connectionContext.socket.send(JSON.stringify(parsedMessage));
    }
  }

  private sendError(connectionContext: ConnectionContext, opId: string | undefined, errorPayload: any,
                    overrideDefaultErrorType?: string): void {
    const sanitizedOverrideDefaultErrorType = overrideDefaultErrorType || MessageTypes.GQL_ERROR;
    if ([
        MessageTypes.GQL_CONNECTION_ERROR,
        MessageTypes.GQL_ERROR,
      ].indexOf(sanitizedOverrideDefaultErrorType) === -1) {
      throw new Error('overrideDefaultErrorType should be one of the allowed error messages' +
        ' GQL_CONNECTION_ERROR or GQL_ERROR');
    }

    this.sendMessage(
      connectionContext,
      opId,
      sanitizedOverrideDefaultErrorType,
      errorPayload,
    );
  }
}

