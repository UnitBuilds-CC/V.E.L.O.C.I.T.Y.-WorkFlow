// Example: Classic SDK Workflow
import { Workflow, Activity, Worker, Client } from '@velocity-workflow/classic';

class OrderWorkflow extends Workflow {
  async execute(orderId: string, amount: number): Promise<any> {
    // Execute activities
    const charge = await this.executeActivity('ChargeActivity', orderId, amount);
    const receipt = await this.executeActivity('ReceiptActivity', orderId);
    
    // Wait for approval signal
    const approved = await this.waitForSignal('approval');
    
    if (approved) {
      const ship = await this.executeActivity('ShipActivity', orderId);
      return { charge, receipt, ship, status: 'completed' };
    } else {
      // Refund
      await this.executeActivity('RefundActivity', orderId);
      return { charge, receipt, status: 'cancelled' };
    }
  }
}

class ChargeActivity extends Activity {
  async execute(orderId: string, amount: number): Promise<any> {
    this.heartbeat({ progress: 'charging' });
    // Process payment
    return { transactionId: `tx-${orderId}`, amount, status: 'charged' };
  }
}

class ReceiptActivity extends Activity {
  async execute(orderId: string): Promise<any> {
    return { receiptId: `rec-${orderId}`, generated: true };
  }
}

class ShipActivity extends Activity {
  async execute(orderId: string): Promise<any> {
    return { trackingId: `track-${orderId}`, shipped: true };
  }
}

class RefundActivity extends Activity {
  async execute(orderId: string): Promise<any> {
    return { refundId: `ref-${orderId}`, refunded: true };
  }
}

// Worker setup
async function setupWorker() {
  const worker = await Worker.create({ taskQueue: 'orders' });
  worker.registerWorkflow(OrderWorkflow);
  worker.registerActivity(ChargeActivity);
  worker.registerActivity(ReceiptActivity);
  worker.registerActivity(ShipActivity);
  worker.registerActivity(RefundActivity);
  await worker.run();
  return worker;
}

// Client usage
async function runWorkflow() {
  const client = new Client({ serverAddress: 'localhost:7233' });
  const execution = await client.startWorkflow('order-123', 'OrderWorkflow', ['order-123', 99.99]);
  
  // Send approval signal
  await client.signal('order-123', 'approval', true);
  
  // Query status
  const status = await client.query('order-123', 'status');
  console.log('Workflow status:', status);
}
