const http = require('http');
const server = http.createServer((req, res) => {
  console.log('req received');
  req.on('close', () => console.log('req close fired!'));
  req.on('aborted', () => console.log('req aborted fired!'));
  setTimeout(() => {
    res.writeHead(200);
    res.end('ok');
    console.log('res sent');
  }, 2000);
});
server.listen(8081, () => {
  const req = http.request('http://127.0.0.1:8081/', { method: 'POST' }, (res) => {
    res.on('data', () => {});
    res.on('end', () => console.log('client got res'));
  });
  req.write('hello');
  req.end();
});
