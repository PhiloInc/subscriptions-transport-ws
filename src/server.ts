import {
  DocumentNode,
  ExecutionResult,
  GraphQLFieldResolver,
  GraphQLSchema,
  parse,
  specifiedRules,
  validate,
  ValidationContext,
} from 'graphql';
import { IncomingMessage } from 'http';
import { createAsyncIterator, forAwaitEach, isAsyncIterable } from 'iterall';
import { nextTick } from 'process';
import * as WebSocket from 'ws';
import { parseLegacyProtocolMessage } from './legacy/parse-legacy-protocol';
import MessageTypes from './message-types';
import { GRAPHQL_SUBSCRIPTIONS, GRAPHQL_WS } from './protocol';
import isObject from './utils/is-object';
import isString from './utils/is-string';
import { isASubscriptionOperation } from './utils/is-subscriptions';

export type ExecutionIterator = AsyncIterator<ExecutionResult>;
export type FormatErrorFunction<TContext = any> = (error: Error, executionParams: ExecutionParams<TContext>) => Error;

export interface ExecutionParams<TContext = any> {
  query: string | DocumentNode;
  variables: { [key: string]: any };
  operationName: string | undefined;
  context: TContext;
  formatResponse?: (executionResult: ExecutionResult, executionParams: ExecutionParams<TContext>) => ExecutionResult;
  formatError?: FormatErrorFunction<TContext>;
  schema?: GraphQLSchema;
}

type OperationId = string | number;

export type InitPromiseResult<TContext = any> = boolean | TContext;

export type OperationContext = {
  executionIterator?: ExecutionIterator;
  messageQueue: OperationMessage[];
  processingStart: boolean;
};

export type ConnectionContext = {
  initPromise: Promise<InitPromiseResult>;
  isLegacy: boolean;
  socket: WebSocket;
  request: IncomingMessage;
  operations: Map<OperationId, OperationContext>;
};

export interface OperationMessagePayload {
  [key: string]: any; // this will support for example any options sent in init like the auth token
  query?: string;
  variables?: { [key: string]: any };
  operationName?: string;
}

export interface OperationMessage {
  payload?: OperationMessagePayload;
  id?: OperationId;
  type: string;
}

export type ExecuteFunction = (
  schema: GraphQLSchema,
  document: DocumentNode,
  rootValue?: any,
  contextValue?: any,
  variableValues?: { [key: string]: any },
  operationName?: string,
  fieldResolver?: GraphQLFieldResolver<any, any>,
) => ExecutionResult | Promise<ExecutionResult> | AsyncIterator<ExecutionResult>;

export type SubscribeFunction = (
  schema: GraphQLSchema,
  document: DocumentNode,
  rootValue?: any,
  contextValue?: any,
  variableValues?: { [key: string]: any },
  operationName?: string,
  fieldResolver?: GraphQLFieldResolver<any, any>,
  subscribeFieldResolver?: GraphQLFieldResolver<any, any>,
) => AsyncIterator<ExecutionResult> | Promise<AsyncIterator<ExecutionResult> | ExecutionResult>;

export type OnDisconnectFunction = (webSocket: WebSocket, connectionContext: ConnectionContext) => void;
export type OnConnectFunction = (
  payload: OperationMessagePayload | undefined,
  webSocket: WebSocket,
  connectionContext: ConnectionContext,
) => InitPromiseResult | Promise<InitPromiseResult>;
export type OnOperationFunction = (
  message: OperationMessage,
  executionParams: ExecutionParams,
  webSocket: WebSocket,
) => ExecutionParams | Promise<ExecutionParams>;
export type OnOperationCompleteFunction = (webSocket: WebSocket, opId: OperationId) => void;

export interface ServerOptions {
  rootValue?: any;
  schema?: GraphQLSchema;
  execute?: ExecuteFunction;
  subscribe?: SubscribeFunction;
  validationRules?: Array<(context: ValidationContext) => any> | ReadonlyArray<any>;
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
  private specifiedRules: Array<(context: ValidationContext) => any> | ReadonlyArray<any>;

  public static create(options: ServerOptions, socketOptionsOrServer: WebSocket.ServerOptions | WebSocket.Server) {
    return new SubscriptionServer(options, socketOptionsOrServer);
  }

  constructor(options: ServerOptions, socketOptionsOrServer: WebSocket.ServerOptions | WebSocket.Server) {
    const { onOperation, onOperationComplete, onConnect, onDisconnect, keepAlive } = options;

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

    const connectionHandler = (socket: WebSocket, request: IncomingMessage) => {
      // Add `upgradeReq` to the socket object to support old API, without creating a memory leak
      // See: https://github.com/websockets/ws/pull/1099
      (socket as any).upgradeReq = request;
      // NOTE: the old GRAPHQL_SUBSCRIPTIONS protocol support should be removed in the future
      if (
        socket.protocol === undefined ||
        (socket.protocol.indexOf(GRAPHQL_WS) === -1 && socket.protocol.indexOf(GRAPHQL_SUBSCRIPTIONS) === -1)
      ) {
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
        operations: new Map(),
      };

      const connectionClosedHandler = (error: any) => {
        if (error) {
          this.sendError(connectionContext, '', { message: error.message ? error.message : error }, MessageTypes.GQL_CONNECTION_ERROR);

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
    };

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

  private unsubscribe(connectionContext: ConnectionContext, opId: OperationId) {
    const operationContext = connectionContext.operations.get(opId);
    if (operationContext) {
      const { executionIterator } = operationContext;
      if (executionIterator && executionIterator.return) {
        executionIterator.return();
      }
      operationContext.executionIterator = undefined;
      if (this.onOperationComplete) {
        this.onOperationComplete(connectionContext.socket, opId);
      }
    }
  }

  private onClose(connectionContext: ConnectionContext) {
    connectionContext.operations.forEach((_, opId) => {
      this.unsubscribe(connectionContext, opId);
    });
  }

  private async processStart(
    connectionContext: ConnectionContext,
    operationContext: OperationContext,
    opId: OperationId,
    message: OperationMessage,
  ) {
    try {
      operationContext.processingStart = true;
      // DR: if a client issued two start requests for the same opId,
      // at this point we allow the 2nd request to replace the first
      if (operationContext.executionIterator) {
        this.unsubscribe(connectionContext, opId);
      }
      const { payload } = message;
      if (!payload) {
        throw new Error('Payload missing');
      }
      const { query } = payload;
      if (!query) {
        throw new Error('query missing');
      }
      const initResult = await connectionContext.initPromise;
      const baseParams: ExecutionParams = {
        query,
        variables: payload.variables || {},
        operationName: payload.operationName,
        context: isObject(initResult) ? Object.assign(Object.create(Object.getPrototypeOf(initResult)), initResult) : {},
        schema: this.schema,
      };
      let promisedParams = Promise.resolve(baseParams);

      if (this.onOperation) {
        promisedParams = Promise.resolve(this.onOperation(message, baseParams, connectionContext.socket));
      }
      const params = await promisedParams;
      if (!isObject(params)) {
        throw new Error('Invalid params returned from onOperation! return values must be an object!');
      }
      if (!params.schema) {
        const error =
          'Missing schema information. The GraphQL schema should be provided either statically in' +
          ' the `SubscriptionServer` constructor or as a property on the object returned from onOperation!';
        throw new Error(error);
      }

      const document = isString(params.query) ? parse(params.query) : params.query;
      let executionPromise: Promise<AsyncIterator<ExecutionResult> | ExecutionResult>;
      const validationErrors = validate(params.schema, document, this.specifiedRules);
      if (validationErrors.length > 0) {
        executionPromise = Promise.resolve({ errors: validationErrors });
      } else {
        let executor: SubscribeFunction | ExecuteFunction = this.execute;
        if (this.subscribe && isASubscriptionOperation(document, params.operationName)) {
          executor = this.subscribe;
        }
        executionPromise = Promise.resolve(
          executor(params.schema, document, this.rootValue, params.context, params.variables, params.operationName),
        );
      }
      const executionResult = await executionPromise;
      const executionIterator = isAsyncIterable(executionResult) ? executionResult : createAsyncIterator([executionResult]);
      operationContext.executionIterator = executionIterator as ExecutionIterator;
      // NOTE: This is a temporary code to support the legacy protocol.
      // As soon as the old protocol has been removed, this code should also be removed.
      this.sendMessage(connectionContext, opId, MessageTypes.SUBSCRIPTION_SUCCESS, undefined);
      operationContext.processingStart = false;
      nextTick(() => {
        this.processQueue(connectionContext, opId);
      });
      await forAwaitEach(executionIterator as any, (value: ExecutionResult) => {
        let result = value;

        if (params.formatResponse) {
          try {
            result = params.formatResponse(value, params);
          } catch (err) {
            console.error('Error in formatError function:', err);
          }
        }
        this.sendMessage(connectionContext, opId, MessageTypes.GQL_DATA, result);
      });
      this.sendMessage(connectionContext, opId, MessageTypes.GQL_COMPLETE, null);
      nextTick(() => {
        this.processQueue(connectionContext, opId);
      });
    } catch (error) {
      operationContext.processingStart = false;
      nextTick(() => {
        this.processQueue(connectionContext, opId);
      });
      if (error.errors) {
        this.sendMessage(connectionContext, opId, MessageTypes.GQL_DATA, {
          errors: error.errors,
        });
      } else {
        this.sendError(connectionContext, opId, { message: error.message });
      }
      this.unsubscribe(connectionContext, opId);
    }
  }

  private processQueue(connectionContext: ConnectionContext, opId: OperationId) {
    const operationContext = connectionContext.operations.get(opId);
    if (!operationContext || operationContext.processingStart) {
      return;
    }
    const message = operationContext.messageQueue.shift();
    if (!message) {
      return;
    }
    const { type } = message;
    if (type === MessageTypes.GQL_STOP) {
      nextTick(() => {
        this.processQueue(connectionContext, opId);
      });
      this.unsubscribe(connectionContext, opId);
      return;
    }
    if (type === MessageTypes.GQL_START) {
      this.processStart(connectionContext, operationContext, opId, message).catch(error => {
        operationContext.processingStart = false;
        nextTick(() => {
          this.processQueue(connectionContext, opId);
        });
        this.sendError(connectionContext, opId, { message: error.message });
      });
      return;
    }
    nextTick(() => {
      this.processQueue(connectionContext, opId);
    });
  }

  private queueMessage(connectionContext: ConnectionContext, opId: OperationId, message: OperationMessage) {
    let operationContext = connectionContext.operations.get(opId);
    if (!operationContext) {
      operationContext = {
        messageQueue: [],
        processingStart: false,
      };
      connectionContext.operations.set(opId, operationContext);
    }
    operationContext.messageQueue.push(message);
    this.processQueue(connectionContext, opId);
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

          connectionContext.initPromise
            .then(result => {
              if (result === false) {
                throw new Error('Prohibited connection!');
              }

              this.sendMessage(connectionContext, undefined, MessageTypes.GQL_CONNECTION_ACK, undefined);

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
            })
            .catch((error: Error) => {
              this.sendError(connectionContext, opId, { message: error.message }, MessageTypes.GQL_CONNECTION_ERROR);

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
          if (opId == null) {
            this.sendError(connectionContext, opId, {
              message: 'Invalid opId!',
            });
            break;
          }
          this.queueMessage(connectionContext, opId, parsedMessage);
          break;

        case MessageTypes.GQL_STOP:
          if (opId == null) {
            this.sendError(connectionContext, opId, {
              message: 'Invalid opId!',
            });
            break;
          }
          this.queueMessage(connectionContext, opId, parsedMessage);
          break;

        default:
          this.sendError(connectionContext, opId, {
            message: 'Invalid message type!',
          });
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

  private sendMessage(connectionContext: ConnectionContext, opId: OperationId | undefined, type: string, payload: any): void {
    const parsedMessage = parseLegacyProtocolMessage(connectionContext, {
      type,
      id: opId,
      payload,
    });

    if (parsedMessage && connectionContext.socket.readyState === WebSocket.OPEN) {
      connectionContext.socket.send(JSON.stringify(parsedMessage));
    }
  }

  private sendError(
    connectionContext: ConnectionContext,
    opId: OperationId | undefined,
    errorPayload: any,
    overrideDefaultErrorType?: string,
  ): void {
    const sanitizedOverrideDefaultErrorType = overrideDefaultErrorType || MessageTypes.GQL_ERROR;
    if ([MessageTypes.GQL_CONNECTION_ERROR, MessageTypes.GQL_ERROR].indexOf(sanitizedOverrideDefaultErrorType) === -1) {
      throw new Error('overrideDefaultErrorType should be one of the allowed error messages' + ' GQL_CONNECTION_ERROR or GQL_ERROR');
    }

    this.sendMessage(connectionContext, opId, sanitizedOverrideDefaultErrorType, errorPayload);
  }
}
