const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');

const root = __dirname;
const rooms = new Map();

const server = http.createServer((request, response) => {
    const requestedPath = request.url === '/' ? '/index.html' : request.url;
    const filePath = path.join(root, requestedPath.split('?')[0]);
    if (!filePath.startsWith(root) || !fs.existsSync(filePath)) {
        response.writeHead(404);
        response.end('Not found');
        return;
    }

    const contentType = filePath.endsWith('.html') ? 'text/html; charset=utf-8' : 'text/plain';
    response.writeHead(200, { 'Content-Type': contentType });
    fs.createReadStream(filePath).pipe(response);
});

const socketServer = new WebSocket.Server({ server });

function send(socket, message) {
    if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message));
}

function broadcast(room, message, except) {
    room.forEach((client) => {
        if (client !== except) send(client.socket, message);
    });
}

socketServer.on('connection', (socket) => {
    const client = { id: crypto.randomUUID(), socket, roomId: null, name: 'Visitante' };

    socket.on('message', (raw) => {
        let message;
        try {
            message = JSON.parse(raw.toString());
        } catch {
            send(socket, { type: 'error', message: 'Mensagem inválida.' });
            return;
        }

        if (message.type === 'join') {
            const roomId = String(message.roomId || '').toUpperCase();
            const name = String(message.name || 'Visitante').trim().slice(0, 32);
            if (!roomId || !name) return send(socket, { type: 'error', message: 'Sala e nome são obrigatórios.' });
            if (!rooms.has(roomId)) rooms.set(roomId, new Map());
            client.roomId = roomId;
            client.name = name;
            const room = rooms.get(roomId);
            const participants = [...room.values()].map(({ id, name: participantName }) => ({ id, name: participantName }));
            room.set(client.id, client);
            send(socket, { type: 'joined', id: client.id, participants });
            broadcast(room, { type: 'participant-joined', participant: { id: client.id, name } }, client);
            return;
        }

        const room = client.roomId && rooms.get(client.roomId);
        if (!room) return;
        if (message.type === 'signal') {
            const target = room.get(message.to);
            if (target) send(target.socket, { ...message, from: client.id, fromName: client.name });
        } else if (message.type === 'chat') {
            broadcast(room, { ...message, from: client.id, fromName: client.name }, client);
        } else if (message.type === 'presence') {
            broadcast(room, { ...message, from: client.id }, client);
        }
    });

    const leave = () => {
        if (!client.roomId) return;
        const room = rooms.get(client.roomId);
        if (!room) return;
        room.delete(client.id);
        broadcast(room, { type: 'participant-left', id: client.id });
        if (!room.size) rooms.delete(client.roomId);
        client.roomId = null;
    };

    socket.on('close', leave);
    socket.on('error', leave);
});

server.listen(process.env.PORT || 3000, () => {
    console.log('ShareScreen disponível em http://localhost:' + (process.env.PORT || 3000));
});
