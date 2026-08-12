/**
 * Activity definition API
 */

import { ActivityContext } from './types';

export type ActivityFunction<TInput = any, TOutput = any> = (
  ctx: ActivityContext,
  input: TInput
) => Promise<TOutput>;

export class Activity {
  private static activities = new Map<string, ActivityFunction>();

  /**
   * Register an activity function
   */
  static register<TInput = any, TOutput = any>(
    name: string,
    fn: ActivityFunction<TInput, TOutput>
  ): void {
    Activity.activities.set(name, fn as ActivityFunction);
  }

  /**
   * Get a registered activity function
   */
  static get(name: string): ActivityFunction | undefined {
    return Activity.activities.get(name);
  }

  /**
   * Check if an activity is registered
   */
  static has(name: string): boolean {
    return Activity.activities.has(name);
  }

  /**
   * Clear all registered activities (for testing)
   */
  static clear(): void {
    Activity.activities.clear();
  }
}

/**
 * Define an activity
 */
export function defineActivity<TInput = any, TOutput = any>(
  name: string,
  fn: ActivityFunction<TInput, TOutput>
): void {
  Activity.register(name, fn);
}

/**
 * Activity context helpers
 */
export class ActivityHelpers {
  /**
   * Record a heartbeat
   */
  static heartbeat(details?: any): void {
    // In a real implementation, this would send a heartbeat to the server
    console.log('Activity heartbeat:', details);
  }

  /**
   * Get activity info
   */
  static getInfo(): ActivityContext {
    // In a real implementation, this would return the current activity context
    throw new Error('Activity context not available');
  }
}
