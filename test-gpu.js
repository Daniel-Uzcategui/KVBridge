const http = require('http');
http.get('http://localhost:8080/api/stats', (res) => {
  let body = '';
  res.on('data', d => body += d);
  res.on('end', () => {
    const d = JSON.parse(body);
    console.log('Keys:', Object.keys(d));
    console.log('gpu:', JSON.stringify(d.gpu, null, 2));
  });
});
