const fastify = require('fastify')();
fastify.post('/test', async (req, reply) => {
  console.log(Date.now(), 'req received');
  req.raw.on('close', () => console.log(Date.now(), 'req close fired!'));
  req.raw.on('aborted', () => console.log(Date.now(), 'req aborted fired!'));
  
  await new Promise(r => setTimeout(r, 1000));
  console.log(Date.now(), 'returning...');
  process.exit(0);
});
fastify.listen({ port: 8081 }, () => {
  const http = require('http');
  const req = http.request('http://127.0.0.1:8081/test', { method: 'POST', headers: {'Content-Type':'text/plain'} });
  req.on('error', () => {}); // ignore client err
  req.write('hello');
  setTimeout(() => req.destroy(), 500); // client aborts!
});
