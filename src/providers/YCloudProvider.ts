import { ProviderClass } from '@builderbot/bot';
import axios from 'axios';
import fs from 'fs';
import path from 'path';

class YCloudProvider extends ProviderClass {
    globalVendorArgs: any;

    constructor(args: any = {}) {
        super();
        this.globalVendorArgs = args;
    }

    protected initProvider() {
        console.log('[YCloudProvider] Listo. Esperando Webhooks...');
    }

    public async initVendor() {
        this.vendor = {};
        return this.vendor;
    }

    public beforeHttpServerInit() {
    }

    public afterHttpServerInit() {
    }

    public busEvents = () => {
        return [];
    };

    /**
     * Descarga y guarda archivos de media (imágenes, audios, videos, documentos)
     */
    public async saveFile(ctx: any, { path: folderPath }: { path: string }) {
        try {
            // En este provider, el payload contiene el mensaje de YCloud/Meta
            const msg = ctx.payload;
            const media = msg?.image || msg?.video || msg?.audio || msg?.document || msg?.sticker;
            
            if (!media || !media.link) {
                console.warn('[YCloudProvider] No se encontró link de media en el payload.');
                return null;
            }

            const url = media.link;
            const response = await axios.get(url, { responseType: 'arraybuffer' });
            
            const mimeType = media.mime_type || media.mimeType || 'application/octet-stream';
            let extension = '.bin';
            
            if (mimeType.includes('image/jpeg')) extension = '.jpg';
            else if (mimeType.includes('image/png')) extension = '.png';
            else if (mimeType.includes('audio/ogg')) extension = '.ogg';
            else if (mimeType.includes('audio')) extension = '.ogg'; // Default para voz
            else if (mimeType.includes('video/mp4')) extension = '.mp4';
            else if (mimeType.includes('pdf')) extension = '.pdf';

            const fileName = `media_${Date.now()}${extension}`;
            const fullPath = path.join(process.cwd(), folderPath, fileName);
            
            const dir = path.dirname(fullPath);
            if (!fs.existsSync(dir)) {
                fs.mkdirSync(dir, { recursive: true });
            }
            
            fs.writeFileSync(fullPath, Buffer.from(response.data));
            console.log(`[YCloudProvider] Archivo descargado y guardado en: ${fullPath}`);
            return fullPath;
        } catch (error: any) {
            console.error('[YCloudProvider] Error en saveFile:', error.message);
            return null;
        }
    }

    public async sendMessage(number: string, message: string, options: any = {}): Promise<any> {
        const apiKey = process.env.YCLOUD_API_KEY;
        const fromNumber = process.env.YCLOUD_WABA_NUMBER;

        if (!apiKey) {
            console.error('[YCloudProvider] Error: YCLOUD_API_KEY no definida en variables de entorno.');
            return;
        }

        if (!fromNumber) {
            console.error('[YCloudProvider] Error: YCLOUD_WABA_NUMBER no definida en variables de entorno.');
            return;
        }

        const url = 'https://api.ycloud.com/v2/whatsapp/messages';
        const cleanNumber = number.replace(/\D/g, '');

        const body: any = {
            from: fromNumber.replace(/\D/g, ''),
            to: cleanNumber,
            type: 'text',
            text: { body: message }
        };

        try {
            const response = await axios.post(url, body, {
                headers: {
                    'X-API-Key': apiKey,
                    'Content-Type': 'application/json'
                }
            });
            return response.data;
        } catch (error: any) {
            console.error('[YCloudProvider] Error enviando mensaje:', JSON.stringify(error?.response?.data || error.message, null, 2));
            return Promise.resolve(null);
        }
    }

    public handleWebhook = (req: any, res: any) => {
        try {
            const body = req.body;
            console.log('[YCloudProvider] Webhook recibido:', JSON.stringify(body));

            if (body.type === 'whatsapp.inbound_message.received' && body.whatsappInboundMessage) {
                const msg = body.whatsappInboundMessage;
                const mediaObject = msg.image || msg.video || msg.audio || msg.document || msg.sticker;
                
                const formatedMessage: any = {
                    body: msg.text?.body || 
                          msg.interactive?.button_reply?.title || 
                          msg.interactive?.list_reply?.title || 
                          msg.button?.text || '',
                    from: msg.waId || msg.from.replace('+', ''),
                    phoneNumber: msg.from.replace('+', ''),
                    name: msg.customerProfile?.name || 'User',
                    type: msg.type,
                    media: mediaObject ? {
                        link: mediaObject.link,
                        mimetype: mediaObject.mime_type || mediaObject.mimeType
                    } : null,
                    payload: msg,
                    message: {}
                };

                // Inyectar compatibilidad para flows que esperan estructura de Baileys
                if (msg.location) {
                    formatedMessage.message.location = {
                        degreesLatitude: msg.location.latitude,
                        degreesLongitude: msg.location.longitude
                    };
                }
                if (msg.image) formatedMessage.message.imageMessage = { mimetype: msg.image.mime_type || msg.image.mimeType };
                if (msg.video) formatedMessage.message.videoMessage = { mimetype: msg.video.mime_type || msg.video.mimeType };
                if (msg.document) formatedMessage.message.documentMessage = { mimetype: msg.document.mime_type || msg.document.mimeType };
                if (msg.audio) formatedMessage.message.audioMessage = { mimetype: msg.audio.mime_type || msg.audio.mimeType };

                this.emit('message', formatedMessage);
            } 
            else if (body.object === 'whatsapp_business_account' || body.entry) {
                // ... (Lógica para webhooks directos de Meta si fuera necesario, similar a la anterior)
                body.entry?.forEach((entry: any) => {
                    entry.changes?.forEach((change: any) => {
                        if (change.value?.messages) {
                            change.value.messages.forEach((msg: any) => {
                                const waId = msg.from; 
                                const phoneNumber = waId; 
                                const mediaObject = msg.image || msg.video || msg.audio || msg.document || msg.sticker;
                                
                                const formatedMessage: any = {
                                    body: msg.text?.body || 
                                          msg.interactive?.button_reply?.title || 
                                          msg.interactive?.list_reply?.title || 
                                          msg.button?.text || '',
                                    from: waId,
                                    phoneNumber: phoneNumber,
                                    name: msg.profile?.name || 'User',
                                    type: msg.type,
                                    media: mediaObject ? {
                                        link: mediaObject.link,
                                        mimetype: mediaObject.mime_type || mediaObject.mimeType
                                    } : null,
                                    payload: msg,
                                    message: {}
                                };

                                if (msg.location) {
                                    formatedMessage.message.location = {
                                        degreesLatitude: msg.location.latitude,
                                        degreesLongitude: msg.location.longitude
                                    };
                                }
                                if (msg.image) formatedMessage.message.imageMessage = { mimetype: msg.image.mime_type || msg.image.mimeType };
                                if (msg.video) formatedMessage.message.videoMessage = { mimetype: msg.video.mime_type || msg.video.mimeType };
                                if (msg.document) formatedMessage.message.documentMessage = { mimetype: msg.document.mime_type || msg.document.mimeType };
                                if (msg.audio) formatedMessage.message.audioMessage = { mimetype: msg.audio.mime_type || msg.audio.mimeType };

                                this.emit('message', formatedMessage);
                            });
                        }
                    });
                });
            }

            if (!res.headersSent) {
                res.statusCode = 200;
                res.end('OK');
            }
        } catch (e) {
            console.error('[YCloudProvider] Error parsing webhook:', e);
            if (!res.headersSent) {
                res.statusCode = 500;
                res.end('Error');
            }
        }
    }
}

export { YCloudProvider };
