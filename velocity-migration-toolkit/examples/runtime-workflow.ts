// Example: Runtime SDK Virtual Object
import { VirtualObject, Service, RuntimeServer, Context } from '@velocity-workflow/runtime';

// Virtual Object with state
const OrderProcessor = new VirtualObject('OrderProcessor');

OrderProcessor.addHandler('process', async (ctx: Context, orderId: string, amount: number) => {
  // Get current state
  const status = await ctx.get('status') || 'pending';
  
  // Update state
  await ctx.set('status', 'processing');
  
  // Invoke other services
  const charge = await ctx.invoke('PaymentService', 'charge', orderId, amount);
  const receipt = await ctx.invoke('ReceiptService', 'generate', orderId);
  
  // Wait for awakeable
  const awakeable = ctx.awakeable<{ approved: boolean }>();
  console.log(`Waiting for approval at: ${awakeable.id}`);
  const approval = await awakeable.promise;
  
  if (approval.approved) {
    await ctx.set('status', 'completed');
    return { charge, receipt, status: 'completed' };
  } else {
    await ctx.set('status', 'cancelled');
    await ctx.invoke('PaymentService', 'refund', orderId);
    return { charge, receipt, status: 'cancelled' };
  }
});

// Stateless Service
const PaymentService = new Service('PaymentService');

PaymentService.addHandler('charge', async (ctx: Context, orderId: string, amount: number) => {
  // Process payment
  return { transactionId: `tx-${orderId}`, amount, status: 'charged' };
});

PaymentService.addHandler('refund', async (ctx: Context, orderId: string) => {
  // Process refund
  return { refundId: `ref-${orderId}`, status: 'refunded' };
});

// Stateless Service
const ReceiptService = new Service('ReceiptService');

ReceiptService.addHandler('generate', async (ctx: Context, orderId: string) => {
  return { receiptId: `rec-${orderId}`, generated: true };
});

// Server setup
async function setupServer() {
  const server = new RuntimeServer({
    host: '0.0.0.0',
    port: 9080,
    engineUrl: 'http://localhost:8080',
  });
  
  server.register(OrderProcessor);
  server.register(PaymentService);
  server.register(ReceiptService);
  
  return server;
}

// Client usage
async function runWorkflow() {
  const server = await setupServer();
  
  // Invoke virtual object
  const invocationId = await server.invoke('OrderProcessor', 'process', 'order-123', ['order-123', 99.99]);
  
  // Resolve awakeable externally
  // await server.resolveAwakeable('awakeable-id', { approved: true });
  
  // Check invocation status
  const invocation = server.getInvocation(invocationId);
  console.log('Invocation status:', invocation?.state);
}
