import { executeDbQuery } from "./utils/dbHandler";
// ...existing imports y lógica del bot...
// import { exec } from 'child_process';
import "dotenv/config";
import path from 'path';
import serve from 'serve-static';
import { Server } from 'socket.io';
import fs from 'fs';
import polka from 'polka';
import bodyParser from 'body-parser';
import { createServer } from 'http';
import QRCode from 'qrcode';
// Estado global para encender/apagar el bot
let botEnabled = true;
import { createBot, createProvider, createFlow, addKeyword, EVENTS } from "@builderbot/bot";
import { MemoryDB } from "@builderbot/bot";
import { YCloudProvider } from "./providers/YCloudProvider";
import { BaileysProvider } from "builderbot-provider-sherpa";
import { restoreSessionFromDb, startSessionSync, deleteSessionFromDb, isSessionInDb } from "./utils/sessionSync";
import { toAsk, httpInject } from "@builderbot-plugins/openai-assistants";
import { typing } from "./utils/presence";
import { idleFlow } from "./Flows/idleFlow";
import { welcomeFlowTxt } from "./Flows/welcomeFlowTxt";
import { welcomeFlowVoice } from "./Flows/welcomeFlowVoice";
import { welcomeFlowImg } from "./Flows/welcomeFlowImg";
import { welcomeFlowVideo } from "./Flows/welcomeFlowVideo";
import { welcomeFlowDoc } from "./Flows/welcomeFlowDoc";
import { locationFlow } from "./Flows/locationFlow";
import { welcomeFlowButton } from "./Flows/welcomeFlowButton";
import { AssistantResponseProcessor } from "./utils/AssistantResponseProcessor";
import { updateMain } from "./addModule/updateMain";
import { ErrorReporter } from "./utils/errorReporter";
// import { AssistantBridge } from './utils-web/AssistantBridge';
import { WebChatManager } from './utils-web/WebChatManager';
import { fileURLToPath } from 'url';
import { getArgentinaDatetimeString } from "./utils/ArgentinaTime";
import { RailwayApi } from "./Api-RailWay/Railway";

//import { imgResponseFlow } from "./Flows/imgResponse";
//import { listImg } from "./addModule/listImg";
//import { testAuth } from './utils/test-google-auth.js';

// Definir __dirname para ES modules
const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Instancia global de WebChatManager para sesiones webchat
const webChatManager = new WebChatManager();
// Eliminado: processUserMessageWeb. Usar lógica principal para ambos canales.

/** Puerto en el que se ejecutará el servidor (Railway usa 8080 por defecto) */
const PORT = process.env.PORT || 8080;
/** ID del asistente de OpenAI */
export const ASSISTANT_ID = process.env.ASSISTANT_ID;
const ID_GRUPO_RESUMEN = process.env.ID_GRUPO_RESUMEN ?? "";

const userQueues = new Map();
const userLocks = new Map();


// Listener para generar el archivo QR manualmente cuando se solicite
export let adapterProvider;
export let groupProvider;
let errorReporter;

const TIMEOUT_MS = 30000;

// Control de timeout por usuario para evitar ejecuciones automáticas superpuestas
const userTimeouts = new Map();

// Wrapper seguro para toAsk que SIEMPRE verifica runs activos
export const safeToAsk = async (assistantId: string, message: string, state: any) => {
    const threadId = state && typeof state.get === 'function' && state.get('thread_id');
    if (threadId) {
        try {
            const { waitForActiveRuns } = await import('./utils/AssistantResponseProcessor.js');
            await waitForActiveRuns(threadId);
        } catch (err) {
            console.error('[safeToAsk] Error esperando runs activos:', err);
            await new Promise(r => setTimeout(r, 3000));
        }
    }
    return toAsk(assistantId, message, state);
};

export const getAssistantResponse = async (assistantId, message, state, fallbackMessage, userId, thread_id = null) => {
    // Solo enviar la fecha/hora si es realmente un hilo nuevo (no existe thread_id ni en el argumento ni en el state)
    let effectiveThreadId = thread_id;
    if (!effectiveThreadId && state && typeof state.get === 'function') {
        effectiveThreadId = state.get('thread_id');
    }
    let systemPrompt = "";
    if (!effectiveThreadId) {
        systemPrompt += `Fecha y hora actual: ${getArgentinaDatetimeString()}\n`;
    }
    const finalMessage = systemPrompt + message;
    // Si hay un timeout previo, lo limpiamos
    if (userTimeouts.has(userId)) {
        clearTimeout(userTimeouts.get(userId));
        userTimeouts.delete(userId);
    }

    let timeoutResolve;
    const timeoutPromise = new Promise((resolve) => {
        timeoutResolve = resolve;
        const timeoutId = setTimeout(async () => {
            console.warn("⏱ Timeout alcanzado. Reintentando con mensaje de control...");
            resolve(await safeToAsk(assistantId, fallbackMessage ?? finalMessage, state));
            userTimeouts.delete(userId);
        }, TIMEOUT_MS);
        userTimeouts.set(userId, timeoutId);
    });

    // Lanzamos la petición a OpenAI, pasando thread_id si existe
    const askPromise = safeToAsk(assistantId, finalMessage, state).then((result) => {
        if (userTimeouts.has(userId)) {
            clearTimeout(userTimeouts.get(userId));
            userTimeouts.delete(userId);
        }
        timeoutResolve(result);
        return result;
    });

    return Promise.race([askPromise, timeoutPromise]);
};

export const processUserMessage = async (
    ctx,
    { flowDynamic, state, provider, gotoFlow }
) => {
    const userId = ctx.from;
    const botNumber = (process.env.YCLOUD_WABA_NUMBER || '').replace(/\D/g, '');

    // FILTRO DE SEGURIDAD: Evitar que el bot procese su propio eco (bucles infinitos)
    if (userId.replace(/\D/g, '') === botNumber) {
        console.log('🛑 [Security] Mensaje de eco detectado desde el número del bot. Ignorando.');
        const { stop } = await import('./utils/timeOut.js');
        stop(ctx); // Detiene cualquier timer de inactividad preventivamente
        return;
    }

    await typing(ctx, provider);
    try {
        const body = ctx.body && ctx.body.trim();


        // Comando para encender el bot
        if (body === "#ON#") {
            if (!botEnabled) {
                botEnabled = true;
                await flowDynamic([{ body: "🤖 Bot activado." }]);
            } else {
                await flowDynamic([{ body: "🤖 El bot ya está activado." }]);
            }
            return state;
        }

        // Comando para apagar el bot
        if (body === "#OFF#") {
            if (botEnabled) {
                botEnabled = false;
                await flowDynamic([{ body: "🛑 Bot desactivado. No responderé a más mensajes hasta recibir #ON#." }]);
            } else {
                await flowDynamic([{ body: "🛑 El bot ya está desactivado." }]);
            }
            return state;
        }

        // Comando para actualizar datos desde sheets
        if (body === "#ACTUALIZAR#") {
            try {
                await updateMain();
                await flowDynamic([{ body: "🔄 Datos actualizados desde Google." }]);
            } catch (err) {
                await flowDynamic([{ body: "❌ Error al actualizar datos desde Google." }]);
            }
            return state;
        }

        // Si el bot está apagado, ignorar todo excepto #ON#
        if (!botEnabled) {
            return;
        }

        // Ignorar mensajes de listas de difusión, newsletters, canales o contactos @lid
        if (ctx.from) {
            if (/@broadcast$/.test(ctx.from) || /@newsletter$/.test(ctx.from) || /@channel$/.test(ctx.from)) {
                console.log('Mensaje de difusión/canal ignorado:', ctx.from);
                return;
            }
            if (/@lid$/.test(ctx.from)) {
                console.log('Mensaje de contacto @lid ignorado:', ctx.from);
                // Reportar al admin
                const assistantName = process.env.ASSISTANT_NAME || 'Asistente demo';
                const assistantId = process.env.ASSISTANT_ID || 'ID no definido';
                if (provider && typeof provider.sendMessage === 'function') {
                    await provider.sendMessage(
                        '+5491130792789',
                        `⚠️ Mensaje recibido de contacto @lid (${ctx.from}). El bot no responde a estos contactos. Asistente: ${assistantName} | ID: ${assistantId}`
                    );
                }
                return;
            }
        }

        // Interceptar trigger de imagen antes de pasar al asistente
        // if (body === "#TestImg#") {
        //     // Usar el flow de imagen para responder y detener el flujo
        //     return gotoFlow(imgResponseFlow);
        // }

        // Usar el nuevo wrapper para obtener respuesta y thread_id
        // Para el contexto del asistente usamos el número de teléfono (si está disponible), 
        // pero ctx.from (wa_id) se mantiene para la entrega de mensajes.
        const contextId = ctx.phoneNumber || ctx.from;
        const response = (await getAssistantResponse(ASSISTANT_ID, ctx.body, state, "Por favor, reenvia el msj anterior ya que no llego al usuario.", contextId, ctx.thread_id)) as string;
        console.log('🔍 DEBUG RAW ASSISTANT MSG (WhatsApp):', JSON.stringify(response));

        // Delegar procesamiento al AssistantResponseProcessor (Maneja DB_QUERY y envios)
        await AssistantResponseProcessor.analizarYProcesarRespuestaAsistente(
            response,
            ctx,
            flowDynamic,
            state,
            provider,
            gotoFlow,
            getAssistantResponse,
            ASSISTANT_ID
        );

        // Si es un contacto con nombre, intentamos guardar el nombre (si no lo tenemos)
        // en algún lugar, o manejarlo como variable de sesión.
        // Aquí podrías agregar lógica para actualizar nombre en sheet si el asistente lo extrajo.
        return state;

    } catch (error) {
        console.error("Error al procesar el mensaje del usuario:", error);

        // Enviar reporte de error al grupo de WhatsApp
        await errorReporter.reportError(
            error,
            ctx.from,
            `https://wa.me/${ctx.from}`
        );

        // 📌 Manejo de error: volver al flujo adecuado
        if (ctx.type === EVENTS.VOICE_NOTE) {
            return gotoFlow(welcomeFlowVoice);
        } else if (ctx.type === EVENTS.ACTION) {
            return gotoFlow(welcomeFlowButton);
        } else {
            return gotoFlow(welcomeFlowTxt);
        }
    }
};


const handleQueue = async (userId) => {
    const queue = userQueues.get(userId);

    if (userLocks.get(userId)) return;

    userLocks.set(userId, true);

    while (queue.length > 0) {
        const { ctx, flowDynamic, state, provider, gotoFlow } = queue.shift();
        try {
            await processUserMessage(ctx, { flowDynamic, state, provider, gotoFlow });
        } catch (error) {
            console.error(`Error procesando el mensaje de ${userId}:`, error);
        }
    }

    userLocks.set(userId, false);
    userQueues.delete(userId);
};

// Función auxiliar para verificar el estado de ambos proveedores
const getBotStatus = async () => {
    try {
        // 1. Estado YCloud (Meta)
        const ycloudConfigured = !!(process.env.YCLOUD_API_KEY && process.env.YCLOUD_WABA_NUMBER);
        
        // 2. Estado Motor de Grupos (Baileys)
        const groupsReady = !!(groupProvider?.vendor?.user || groupProvider?.globalVendorArgs?.sock?.user);
        
        const sessionsDir = path.join(process.cwd(), 'bot_sessions');
        let groupsLocalActive = false;
        if (fs.existsSync(sessionsDir)) {
            const files = fs.readdirSync(sessionsDir).filter(f => f.endsWith('.json'));
            groupsLocalActive = files.includes('creds.json');
        }

        const groupsRemoteActive = await isSessionInDb('groups');

        return {
            ycloud: {
                active: ycloudConfigured,
                status: ycloudConfigured ? 'connected' : 'error',
                phoneNumber: process.env.YCLOUD_WABA_NUMBER || null
            },
            groups: {
                initialized: !!groupProvider,
                active: groupsReady,
                source: groupsReady ? 'connected' : (groupsLocalActive ? 'local' : 'none'),
                hasRemote: groupsRemoteActive,
                qr: fs.existsSync(path.join(process.cwd(), 'bot.groups.qr.png')),
                phoneNumber: groupProvider?.vendor?.user?.id?.split(':')[0] || null
            }
        };
    } catch (e) {
        console.error('[Status] Error obteniendo estado:', e);
        return { error: String(e) };
    }
};

// Main function to initialize the bot and load Google Sheets data
const main = async () => {
    // 0. Ejecutar script de inicialización de funciones (solo si no existen)


    // 1. Limpiar QR antiguo al inicio
    const qrPath = path.join(process.cwd(), 'bot.qr.png');
    if (fs.existsSync(qrPath)) {
        try {
            fs.unlinkSync(qrPath);
            console.log('🗑️ [Init] QR antiguo eliminado.');
        } catch (e) {
            console.error('⚠️ [Init] No se pudo eliminar QR antiguo:', e);
        }
    }

    // 2. Restaurar sesión de grupos desde DB
    try {
        await restoreSessionFromDb('groups');
        // Pequeña espera para asegurar que el sistema de archivos se asiente
        await new Promise(resolve => setTimeout(resolve, 2000));
    } catch (e) {
        console.error('[Init] Error restaurando sesión de grupos:', e);
    }

    // 3. Inicializar Provider Principal (YCloud)
    adapterProvider = createProvider(YCloudProvider, {});

    // 4. Inicializar Provider Secundario (Grupos - Baileys)
    try {
        console.log('📡 [GroupSync] Creando instancia de motor de grupos (Baileys)...');
        
        // Verificar archivos existentes para diagnóstico
        const sessionsDir = path.join(process.cwd(), 'bot_sessions');
        if (fs.existsSync(sessionsDir)) {
            console.log('📂 [GroupSync] Archivos en bot_sessions:', fs.readdirSync(sessionsDir));
        }

        groupProvider = createProvider(BaileysProvider, {
            version: [2, 3000, 1030817285],
            groupsIgnore: false,
            readStatus: false,
            disableHttpServer: true
        });

        // Capturar TODOS los eventos para diagnóstico
        const groupBus = (groupProvider as any).bus;
        if (groupBus) {
            groupBus.on('*', (event: any, payload: any) => {
                console.log(`[GroupSync Bus] Evento detectado: ${event}`);
            });
        }

        // Configurar listeners redundantes para QR
        const handleQR = async (qrString: string) => {
            if (qrString) {
                console.log(`⚡ [GroupSync] QR detectado (largo: ${qrString.length}). Generando bot.groups.qr.png...`);
                const qrPath = path.join(process.cwd(), 'bot.groups.qr.png');
                await QRCode.toFile(qrPath, qrString, { scale: 10, margin: 2 });
                console.log(`✅ [GroupSync] QR guardado en ${qrPath}`);
            }
        };

        groupProvider.on('require_action', async (payload: any) => {
            console.log('⚡ [GroupSync] require_action received.');
            const qr = (typeof payload === 'string') ? payload : (payload?.qr || payload?.payload?.qr || payload?.code);
            await handleQR(qr);
        });

        groupProvider.on('qr', async (qr: string) => {
            console.log('⚡ [GroupSync] event qr received.');
            await handleQR(qr);
        });

        groupProvider.on('auth_require', async (qr: string) => {
            console.log('⚡ [GroupSync] event auth_require received.');
            await handleQR(qr);
        });

        groupProvider.on('ready', () => {
             console.log('✅ [GroupSync] Motor de grupos conectado satisfactoriamente.');
             const qrPath = path.join(process.cwd(), 'bot.groups.qr.png');
             if (fs.existsSync(qrPath)) fs.unlinkSync(qrPath);
        });

        // Forzar arranque del motor secundario
        console.log('📡 [GroupSync] Iniciando vendor...');
        setTimeout(async () => {
            try {
                if (groupProvider.initVendor) {
                    await groupProvider.initVendor();
                    console.log('📡 [GroupSync] initVendor ejecutado.');
                } else if ((groupProvider as any).init) {
                    await (groupProvider as any).init();
                }
            } catch (err) {
                console.error('❌ [GroupSync] Error al llamar initVendor:', err);
            }
        }, 1000);

        groupProvider.on('message', () => {}); 

    } catch (e) {
        console.error('❌ [GroupSync] Error crítico en motor de grupos:', e);
    }

    // 4. Listeners del Provider
    adapterProvider.on('require_action', async (payload: any) => {
        console.log('⚡ [Provider] require_action received. Payload:', payload);
        let qrString = null;
        if (typeof payload === 'string') {
            qrString = payload;
        } else if (payload && typeof payload === 'object') {
            if (payload.qr) qrString = payload.qr;
            else if (payload.code) qrString = payload.code;
        }
        if (qrString && typeof qrString === 'string') {
            console.log('⚡ [Provider] QR Code detected (length: ' + qrString.length + '). Generating image...');
            try {
                const qrPath = path.join(process.cwd(), 'bot.qr.png');
                await QRCode.toFile(qrPath, qrString, {
                    color: { dark: '#000000', light: '#ffffff' },
                    scale: 4,
                    margin: 2
                });
                console.log(`✅ [Provider] QR Image saved to ${qrPath}`);
            } catch (err) {
                console.error('❌ [Provider] Error generating QR image:', err);
            }
        }
    });

    adapterProvider.on('message', (ctx) => {
        console.log(`Type Msj Recibido: ${ctx.type || 'desconocido'}`);
        console.log('⚡ [Provider] message received');
        
        // Detección de botones para Sherpa/Baileys (ctx.message)
        // Y para YCloud (ctx.type === 'interactive' o presence de payload)
        const isBaileysButton = ctx.message?.buttonsResponseMessage || 
                                ctx.message?.templateButtonReplyMessage || 
                                ctx.message?.interactiveResponseMessage;
        
        const isYCloudButton = ctx.type === 'interactive' || ctx.type === 'button';

        if (isBaileysButton || isYCloudButton) {
            console.log('🔘 Interacción de botón detectada');
            
            if (isBaileysButton) {
                // Mapear el texto del botón al body para que el flujo pueda procesarlo
                if (ctx.message?.buttonsResponseMessage) {
                    ctx.body = ctx.message.buttonsResponseMessage.selectedDisplayText;
                } else if (ctx.message?.templateButtonReplyMessage) {
                    ctx.body = ctx.message.templateButtonReplyMessage.selectedDisplayText;
                } else if (ctx.message?.interactiveResponseMessage) {
                    try {
                        const interactive = JSON.parse(ctx.message.interactiveResponseMessage.nativeFlowResponseMessage.paramsJson);
                        ctx.body = interactive.id;
                    } catch (e) {
                        ctx.body = 'buttonInteraction';
                    }
                }
            }
            
            // Asignar el tipo ACTION para disparar welcomeFlowButton
            ctx.type = EVENTS.ACTION;
            console.log(`Updated Type Msj Recibido (Button): ${ctx.type}`);
        }

        // Normalización de tipos para YCloud (Media y otros eventos)
        if (ctx.type === 'audio') {
            // En YCloud, ctx.payload es whatsappInboundMessage
            const isVoice = ctx.payload?.audio?.voice;
            ctx.type = isVoice ? EVENTS.VOICE_NOTE : EVENTS.MEDIA;
            console.log(`Updated Type Msj Recibido (Audio): ${ctx.type} (isVoice: ${isVoice})`);
        } else if (ctx.type === 'image' || ctx.type === 'video') {
            ctx.type = EVENTS.MEDIA;
            console.log(`Updated Type Msj Recibido (Media): ${ctx.type}`);
        } else if (ctx.type === 'document') {
            ctx.type = EVENTS.DOCUMENT;
            console.log(`Updated Type Msj Recibido (Document): ${ctx.type}`);
        } else if (ctx.type === 'location') {
            ctx.type = EVENTS.LOCATION;
            console.log(`Updated Type Msj Recibido (Location): ${ctx.type}`);
        }
    });
    adapterProvider.on('ready', () => {
        console.log('✅ [Provider] READY: El bot está conectado y operativo.');
    });
    adapterProvider.on('auth_failure', (payload) => {
        console.log('❌ [Provider] AUTH_FAILURE: Error de autenticación.', payload);
    });

    // Evento adicional para detectar desconexiones
    adapterProvider.on('host_failure', (payload) => {
        console.log('⚠️ [Provider] HOST_FAILURE: Problema de conexión con WhatsApp.', payload);
    });

    errorReporter = new ErrorReporter(adapterProvider, ID_GRUPO_RESUMEN);

    console.log("📌 Inicializando datos desde Google Sheets...");
    await updateMain();

    console.log('🚀 [Init] Iniciando createBot...');
    const adapterFlow = createFlow([welcomeFlowTxt, welcomeFlowVoice, welcomeFlowImg, welcomeFlowVideo, welcomeFlowDoc, locationFlow, idleFlow, welcomeFlowButton]);
    const adapterDB = new MemoryDB();

    const { httpServer } = await createBot({
        flow: adapterFlow,
        provider: adapterProvider,
        database: adapterDB,
    });

    console.log('🔍 [DEBUG] createBot httpServer:', !!httpServer);
    console.log('🔍 [DEBUG] adapterProvider.server:', !!adapterProvider.server);

    // Iniciar sincronización periódica de sesión hacia Supabase (Solo para grupos)
    startSessionSync('groups');

    // Inicializar servidor Polka propio para WebChat y QR
    const app = adapterProvider.server;

    // Middleware para parsear JSON en el body
    app.use(bodyParser.json());

    // 1. Middleware de compatibilidad (res.json, res.send, res.sendFile, etc)
    app.use((req, res, next) => {
        res.status = (code) => { res.statusCode = code; return res; };
        res.send = (body) => {
            if (res.headersSent) return res;
            if (typeof body === 'object') {
                res.setHeader('Content-Type', 'application/json');
                res.end(JSON.stringify(body || null));
            } else {
                res.end(body || '');
            }
            return res;
        };
        res.json = (data) => {
            if (res.headersSent) return res;
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify(data || null));
            return res;
        };
        res.sendFile = (filepath) => {
            if (res.headersSent) return;
            try {
                if (fs.existsSync(filepath)) {
                    const ext = path.extname(filepath).toLowerCase();
                    const mimeTypes = {
                        '.html': 'text/html',
                        '.js': 'application/javascript',
                        '.css': 'text/css',
                        '.png': 'image/png',
                        '.jpg': 'image/jpeg',
                        '.gif': 'image/gif',
                        '.svg': 'image/svg+xml',
                        '.json': 'application/json'
                    };
                    res.setHeader('Content-Type', mimeTypes[ext] || 'application/octet-stream');
                    fs.createReadStream(filepath)
                        .on('error', (err) => {
                            console.error(`[ERROR] Stream error in sendFile (${filepath}):`, err);
                            if (!res.headersSent) {
                                res.statusCode = 500;
                                res.end('Internal Server Error');
                            }
                        })
                        .pipe(res);
                } else {
                    console.error(`[ERROR] sendFile: File not found: ${filepath}`);
                    res.statusCode = 404;
                    res.end('Not Found');
                }
            } catch (e) {
                console.error(`[ERROR] Error in sendFile (${filepath}):`, e);
                if (!res.headersSent) {
                    res.statusCode = 500;
                    res.end('Internal Error');
                }
            }
        };
        next();
    });

    // 2. Middleware de logging y redirección de raíz
    app.use((req, res, next) => {
        console.log(`[REQUEST] ${req.method} ${req.url}`);
        try {
            if (req.url === "/" || req.url === "") {
                console.log('[DEBUG] Redirigiendo raíz (/) a /dashboard via middleware');
                res.writeHead(302, { 'Location': '/dashboard' });
                return res.end();
            }
            next();
        } catch (err) {
            console.error('❌ [ERROR] Crash en cadena de middleware:', err);
            if (!res.headersSent) {
                res.statusCode = 500;
                res.end('Internal Server Error');
            }
        }
    });

    // 3. Función para servir páginas HTML
    function serveHtmlPage(route, filename) {
        const handler = (req, res) => {
            console.log(`[DEBUG] Serving HTML for ${req.url} -> ${filename}`);
            try {
                const possiblePaths = [
                    path.join(process.cwd(), 'src', 'html', filename),
                    path.join(process.cwd(), filename),
                    path.join(process.cwd(), 'src', filename),
                    path.join(__dirname, 'html', filename),
                    path.join(__dirname, filename),
                    path.join(__dirname, '..', 'src', 'html', filename)
                ];

                let htmlPath = null;
                for (const p of possiblePaths) {
                    if (fs.existsSync(p) && fs.lstatSync(p).isFile()) {
                        htmlPath = p;
                        break;
                    }
                }

                if (htmlPath) {
                    res.sendFile(htmlPath);
                } else {
                    console.error(`[ERROR] File not found: ${filename}`);
                    res.status(404).send('HTML no encontrado en el servidor');
                }
            } catch (err) {
                console.error(`[ERROR] Failed to serve ${filename}:`, err);
                res.status(500).send('Error interno al servir HTML');
            }
        };
        app.get(route, handler);
        if (route !== "/") {
            app.get(route + '/', handler);
        }
    }

    // Registrar Webhook para YCloud/Meta
    app.post('/webhook', (req, res) => {
        adapterProvider.handleWebhook(req, res);
    });

    // Inyectar rutas del plugin
    httpInject(app);

    // Registrar páginas HTML
    serveHtmlPage("/dashboard", "dashboard.html");
    serveHtmlPage("/webchat", "webchat.html");
    serveHtmlPage("/webreset", "webreset.html");
    serveHtmlPage("/variables", "variables.html");

    // Servir archivos estáticos
    app.use("/js", serve(path.join(process.cwd(), "src", "js")));
    app.use("/style", serve(path.join(process.cwd(), "src", "style")));
    app.use("/assets", serve(path.join(process.cwd(), "src", "assets")));
    // Servir el código QR Principal (YCloud Webhook QR si existiera)
    app.get("/qr.png", (req, res) => {
        const qrPath = path.join(process.cwd(), 'bot.qr.png');
        if (fs.existsSync(qrPath)) {
            res.setHeader('Content-Type', 'image/png');
            fs.createReadStream(qrPath).pipe(res);
        } else {
            res.statusCode = 404;
            res.end('QR not found');
        }
    });

    // Servir el código QR de Grupos (Baileys)
    app.get("/qr-groups.png", (req, res) => {
        const qrPath = path.join(process.cwd(), 'bot.groups.qr.png');
        if (fs.existsSync(qrPath)) {
            res.setHeader('Content-Type', 'image/png');
            fs.createReadStream(qrPath).pipe(res);
        } else {
            res.statusCode = 404;
            res.end('QR not found');
        }
    });

    // API Endpoints
    app.get('/api/assistant-name', (req, res) => {
        const assistantName = process.env.ASSISTANT_NAME || 'Asistente demo';
        res.json({ name: assistantName });
    });

    app.get('/api/dashboard-status', async (req, res) => {
        const status = await getBotStatus();
        res.json(status);
    });

    app.post('/api/delete-session', async (req, res) => {
        try {
            await deleteSessionFromDb();
            res.json({ success: true });
        } catch (err) {
            console.error('Error en /api/delete-session:', err);
            res.status(500).json({ success: false, error: err instanceof Error ? err.message : String(err) });
        }
    });

    app.post("/api/restart-bot", async (req, res) => {
        console.log('POST /api/restart-bot recibido');
        try {
            const result = await RailwayApi.restartActiveDeployment();
            if (result.success) {
                res.json({ success: true, message: "Reinicio solicitado correctamente." });
            } else {
                res.status(500).json({ success: false, error: result.error || "Error desconocido" });
            }
        } catch (err: any) {
            console.error('Error en /api/restart-bot:', err);
            res.status(500).json({ success: false, error: err.message });
        }
    });

    app.get("/api/variables", async (req, res) => {
        try {
            const variables = await RailwayApi.getVariables();
            if (variables) {
                res.json({ success: true, variables });
            } else {
                res.status(500).json({ success: false, error: "No se pudieron obtener las variables de Railway" });
            }
        } catch (err: any) {
            console.error('Error en GET /api/variables:', err);
            res.status(500).json({ success: false, error: err.message });
        }
    });

    app.post("/api/update-variables", async (req, res) => {
        try {
            const { variables } = req.body;
            if (!variables || typeof variables !== 'object') {
                return res.status(400).json({ success: false, error: "Variables no proporcionadas o formato inválido" });
            }

            console.log("[API] Actualizando variables en Railway...");
            const updateResult = await RailwayApi.updateVariables(variables);

            if (!updateResult.success) {
                return res.status(500).json({ success: false, error: updateResult.error });
            }

            console.log("[API] Variables actualizadas. Solicitando reinicio...");
            const restartResult = await RailwayApi.restartActiveDeployment();

            if (restartResult.success) {
                res.json({ success: true, message: "Variables actualizadas y reinicio solicitado." });
            } else {
                res.json({ success: true, message: "Variables actualizadas, pero falló el reinicio automático.", warning: restartResult.error });
            }
        } catch (err: any) {
            console.error('Error en POST /api/update-variables:', err);
            res.status(500).json({ success: false, error: err.message });
        }
    });

    // Socket.IO initialization function
    const initSocketIO = (serverInstance) => {
        if (!serverInstance) {
            console.error('❌ [ERROR] No se pudo obtener serverInstance para Socket.IO');
            return;
        }
        console.log('✅ [DEBUG] Inicializando Socket.IO...');
        const io = new Server(serverInstance, { cors: { origin: '*' } });
        io.on('connection', (socket) => {
            console.log('💬 Cliente web conectado');
            socket.on('message', async (msg) => {
                try {
                    let ip = '';
                    const xff = socket.handshake.headers['x-forwarded-for'];
                    if (typeof xff === 'string') ip = xff.split(',')[0];
                    else if (Array.isArray(xff)) ip = xff[0];
                    else ip = socket.handshake.address || '';

                    if (!global.webchatHistories) global.webchatHistories = {};
                    const historyKey = `webchat_${ip}`;
                    if (!global.webchatHistories[historyKey]) global.webchatHistories[historyKey] = [];
                    const _history = global.webchatHistories[historyKey];

                    const state = {
                        get: (key) => key === 'history' ? _history : undefined,
                        update: async (msg, role = 'user') => {
                            _history.push({ role, content: msg });
                            if (_history.length > 10) _history.shift();
                        },
                        clear: async () => { _history.length = 0; }
                    };

                    let replyText = '';
                    const flowDynamic = async (arr) => {
                        if (Array.isArray(arr)) replyText = arr.map(a => a.body).join('\n');
                        else if (typeof arr === 'string') replyText = arr;
                    };

                    if (msg.trim().toLowerCase() === "#reset") {
                        await state.clear();
                        replyText = "🔄 Chat reiniciado.";
                    } else {
                        await processUserMessage({ from: ip, body: msg, type: 'webchat' }, { flowDynamic, state, provider: undefined, gotoFlow: () => { } });
                    }
                    socket.emit('reply', replyText);
                } catch (err) {
                    console.error('Error Socket.IO:', err);
                    socket.emit('reply', 'Error procesando mensaje.');
                }
            });
        });
    };

    app.post('/webchat-api', async (req, res) => {
        if (!req.body || !req.body.message) {
            return res.status(400).json({ error: "Falta 'message'" });
        }
        try {
            const message = req.body.message;
            let ip = '';
            const xff = req.headers['x-forwarded-for'];
            if (typeof xff === 'string') ip = xff.split(',')[0];
            else ip = req.ip || '';

            const { getOrCreateThreadId, sendMessageToThread, deleteThread } = await import('./utils-web/openaiThreadBridge');
            const session = webChatManager.getSession(ip);
            let replyText = '';

            if (message.trim().toLowerCase() === "#reset") {
                await deleteThread(session);
                session.clear();
                replyText = "🔄 Chat reiniciado.";
            } else {
                const threadId = await getOrCreateThreadId(session);
                session.addUserMessage(message);

                const state = {
                    get: (key) => key === 'thread_id' ? session.thread_id : undefined,
                    update: async () => { },
                    clear: async () => session.clear(),
                };

                const webChatAdapterFn = async (assistantId, message, state, fallback, userId, threadId) => {
                    return await sendMessageToThread(threadId, message, assistantId);
                };

                const reply = await webChatAdapterFn(ASSISTANT_ID, message, state, "", ip, threadId);

                const flowDynamic = async (arr) => {
                    const text = Array.isArray(arr) ? arr.map(a => a.body).join('\n') : arr;
                    replyText = replyText ? replyText + "\n\n" + text : text;
                };

                await AssistantResponseProcessor.analizarYProcesarRespuestaAsistente(
                    reply,
                    { type: 'webchat', from: ip, thread_id: threadId, body: message },
                    flowDynamic,
                    state,
                    undefined,
                    () => { },
                    webChatAdapterFn,
                    ASSISTANT_ID
                );
                session.addAssistantMessage(replyText);
            }
            res.json({ reply: replyText });
        } catch (err) {
            console.error('Error /webchat-api:', err);
            res.status(500).json({ reply: 'Error interno.' });
        }
    });

    // Log informativo para configuración de Webhook
    const webhookUrl = `${process.env.PROJECT_URL || 'https://tu-url-de-railway.up.railway.app'}/webhook`;
    console.log('🌐 [Webhook] Configura esta URL en YCloud/Meta:');
    console.log(`🔗 URL: ${webhookUrl}`);
    console.log('✅ [Webhook] Evento a suscribir: whatsapp.inbound_message.received');

    // Iniciar servidor
    try {
        console.log(`🚀 [INFO] Iniciando servidor en puerto ${PORT}...`);
        httpServer(+PORT);
        console.log(`✅ [INFO] Servidor escuchando en puerto ${PORT}`);
        if (app.server) {
            initSocketIO(app.server);
        }
    } catch (err) {
        console.error('❌ [ERROR] Error al iniciar servidor:', err);
    }

    console.log('✅ [INFO] Main function completed');
};

process.on('unhandledRejection', (reason, promise) => {
    console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (error) => {
    console.error('❌ Uncaught Exception:', error);
    // Opcional: reiniciar proceso si es crítico
    // process.exit(1);
});

export {
    welcomeFlowTxt, welcomeFlowVoice, welcomeFlowImg, welcomeFlowVideo, welcomeFlowDoc, locationFlow,
    handleQueue, userQueues, userLocks,
};

main().catch(err => {
    console.error('❌ [FATAL] Error en la función main:', err);
});

//ok
//version funcional - primer version - 18/02/2026. 23:20