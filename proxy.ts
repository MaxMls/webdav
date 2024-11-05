import http from 'http';
import httpProxy from 'http-proxy';

const proxy = httpProxy.createProxyServer({});

const server = http.createServer((req, res) => {
    // change the target URL as needed
    proxy.web(req, res, { target: 'http://localhost:3000' }, (error) => {
        console.error('Proxy error:', error);
        res.writeHead(502);
        res.end('Bad Gateway');
    });
});

const PORT = 5000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Proxy server is running on http://0.0.0.0:${PORT}`);
});