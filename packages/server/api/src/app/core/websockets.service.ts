import { rejectedPromiseHandler } from '@activepieces/server-shared'
import { ActivepiecesError, ErrorCode, isNil, Principal, PrincipalForType, PrincipalType, WebsocketServerEvent } from '@activepieces/shared'
import { FastifyBaseLogger } from 'fastify'
import { Socket } from 'socket.io'
import { accessTokenManager } from '../authentication/lib/access-token-manager'
import { projectMemberService } from '../ee/projects/project-members/project-member.service'
import { app } from '../server'

export type WebsocketListener<T, PR extends PrincipalType.USER | PrincipalType.WORKER> = (socket: Socket) => (data: T, principal: PrincipalForType<PR>, projectId: PR extends PrincipalType.USER ? string : null, callback?: (data: unknown) => void) => Promise<void>

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type ListenerMap<PR extends PrincipalType.USER | PrincipalType.WORKER> = Partial<Record<WebsocketServerEvent, WebsocketListener<any, PR>>>

const listener = {
    [PrincipalType.USER]: {} as ListenerMap<PrincipalType.USER>,
    [PrincipalType.WORKER]: {} as ListenerMap<PrincipalType.WORKER>,
}

export const websocketService = {
    to: (workerId: string) => app!.io.to(workerId),
    async init(socket: Socket, log: FastifyBaseLogger): Promise<void> {
        const principal = await websocketService.verifyPrincipal(socket)
        const type = principal.type
        if (![PrincipalType.USER, PrincipalType.WORKER].includes(type)) {
            return
        }

        const castedType = type as keyof typeof listener
        const projectId = socket.handshake.auth.projectId
        switch (type) {
            case PrincipalType.USER: {
                await validateProjectId({ userId: principal.id, projectId, log })
                log.info({
                    message: 'User connected',
                    userId: principal.id,
                    projectId,
                })
                await socket.join(projectId)
                await socket.join(principal.id)
                break
            }
            case PrincipalType.WORKER: {
                const workerId = socket.handshake.auth.workerId
                log.info({
                    message: 'Worker connected',
                    workerId,
                })
                await socket.join(workerId)
                break
            }
            default: {
                throw new ActivepiecesError({
                    code: ErrorCode.AUTHENTICATION,
                    params: {
                        message: 'Invalid principal type',
                    },
                })
            }
        }

        // Add socket-level error handler
        socket.on('error', (error) => {
            log.error({
                message: 'Socket error occurred',
                error: error instanceof Error ? error.message : String(error),
                principalId: principal.id,
                projectId,
            })
        })

        // Register all event listeners with safe error handling
        for (const [event, handler] of Object.entries(listener[castedType])) {
            socket.on(event, async (data, callback) => {
                try {
                    // Wrap handler execution with error protection
                    const handlerPromise = handler(socket)(data, principal, projectId, callback)
                    
                    // Use rejectedPromiseHandler if it handles promise rejections
                    // and also wrap it to catch synchronous errors
                    await rejectedPromiseHandler(handlerPromise, log)
                } catch (error) {
                    // This catches synchronous errors and any unhandled promise rejections
                    log.error({
                        message: `Websocket handler failed for event: ${event}`,
                        error: error instanceof Error ? {
                            message: error.message,
                            stack: error.stack,
                            name: error.name
                        } : String(error),
                        principalId: principal.id,
                        projectId,
                        event,
                        data: JSON.stringify(data).substring(0, 500), // Limit data size in logs
                    })
                    
                    // Send error response to client if callback is provided
                    if (typeof callback === 'function') {
                        try {
                            callback({ 
                                success: false, 
                                error: 'Internal server error in websocket handler',
                                timestamp: new Date().toISOString()
                            })
                        } catch (callbackError) {
                            log.error('Failed to send error callback to client', { 
                                callbackError: callbackError instanceof Error ? callbackError.message : String(callbackError),
                                event,
                                principalId: principal.id
                            })
                        }
                    }
                }
            })
        }

        // Handle CONNECT event with error protection
        for (const handler of Object.values(listener[castedType][WebsocketServerEvent.CONNECT] ?? {})) {
            try {
                handler(socket)
            } catch (error) {
                log.error({
                    message: 'Websocket CONNECT handler failed',
                    error: error instanceof Error ? error.message : String(error),
                    principalId: principal.id,
                    projectId,
                })
            }
        }
    },
    async onDisconnect(socket: Socket): Promise<void> {
        const principal = await websocketService.verifyPrincipal(socket)
        const castedType = principal.type as keyof typeof listener
        
        // Handle DISCONNECT event with error protection
        for (const handler of Object.values(listener[castedType][WebsocketServerEvent.DISCONNECT] ?? {})) {
            try {
                handler(socket)
            } catch (error) {
                // Note: logger not available in onDisconnect context, use console
                // In a real scenario, you might want to pass logger through
                console.error('Websocket DISCONNECT handler failed:', {
                    error: error instanceof Error ? error.message : String(error),
                    principalId: principal.id,
                    principalType: principal.type,
                })
            }
        }
    },
    async verifyPrincipal(socket: Socket): Promise<Principal> {
        try {
            return await accessTokenManager.verifyPrincipal(socket.handshake.auth.token)
        } catch (error) {
            throw new ActivepiecesError({
                code: ErrorCode.AUTHENTICATION,
                params: {
                    message: 'Invalid authentication token',
                    details: error instanceof Error ? error.message : String(error)
                },
            })
        }
    },
    addListener<T, PR extends PrincipalType.WORKER | PrincipalType.USER>(principalType: PR, event: WebsocketServerEvent, handler: WebsocketListener<T, PR>): void {
        switch (principalType) {
            case PrincipalType.USER: {
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                listener[PrincipalType.USER][event] = handler as unknown as WebsocketListener<any, PrincipalType.USER>
                break
            }
            case PrincipalType.WORKER: {
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                listener[PrincipalType.WORKER][event] = handler as unknown as WebsocketListener<any, PrincipalType.WORKER>
                break
            }
        }
    },
    emitWithAck<T = unknown>(event: WebsocketServerEvent, workerId: string, data?: unknown): Promise<T> {
        try {
            return app!.io.to([workerId]).timeout(4000).emitWithAck(event, data)
        } catch (error) {
            // Log emit failures
            console.error('Websocket emitWithAck failed:', {
                error: error instanceof Error ? error.message : String(error),
                event,
                workerId,
            })
            throw error
        }
    },
}

const validateProjectId = async ({ userId, projectId, log }: ValidateProjectIdArgs): Promise<void> => {
    if (isNil(projectId)) {
        throw new ActivepiecesError({
            code: ErrorCode.AUTHENTICATION,
            params: {
                message: 'Project ID is required',
            },
        })
    }
    
    try {
        const role = await projectMemberService(log).getRole({
            projectId,
            userId,
        })

        if (isNil(role)) {
            throw new ActivepiecesError({
                code: ErrorCode.AUTHORIZATION,
                params: {
                    message: 'User not allowed to access this project',
                },
            })
        }
    } catch (error) {
        if (error instanceof ActivepiecesError) {
            throw error
        }
        // Wrap unknown errors
        throw new ActivepiecesError({
            code: ErrorCode.AUTHORIZATION,
            params: {
                message: 'Failed to validate project access',
                details: error instanceof Error ? error.message : String(error)
            },
        })
    }
}

type ValidateProjectIdArgs = {
    userId: string
    projectId?: string
    log: FastifyBaseLogger
}
