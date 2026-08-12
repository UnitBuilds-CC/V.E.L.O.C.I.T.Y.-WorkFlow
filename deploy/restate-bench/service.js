// Restate Benchmark Service — mirrors velocity-bench workloads
const http = require('http');

// Simple HTTP server that simulates workflow operations
// This runs INSIDE the Restate container network, benchmarking Restate's ingress

const PORT = 9080;
let requestCount = 0;

const server = http.createServer((req, res) => {
  requestCount++;
  
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok' }));
    return;
  }
  
  if (req.url === '/stats') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ requestCount }));
    return;
  }
  
  // Simulate workflow: parse steps from URL
  // /workflow?steps=10 — simulate N-step workflow
  const url = new URL(req.url, `http://localhost:${PORT}`);
  
  if (url.pathname === '/workflow') {
    const steps = parseInt(url.searchParams.get('steps') || '10');
    const payload = url.searchParams.get('payload') || '';
    
    // Simulate step execution (no actual work, just processing)
    let result = { steps: steps, status: 'completed' };
    
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(result));
    return;
  }
  
  // /signal — simulate signal delivery
  if (url.pathname === '/signal') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'signaled' }));
    return;
  }
  
  // /query — simulate query
  if (url.pathname === '/query') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'running', step: 5 }));
    return;
  }
  
  res.writeHead(404);
  res.end('Not found');
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Benchmark service listening on 0.0.0.0:${PORT}`);
});
