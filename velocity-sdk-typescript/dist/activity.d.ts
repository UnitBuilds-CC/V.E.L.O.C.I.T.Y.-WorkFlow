/**
 * Activity definition API
 */
import { ActivityContext } from './types';
export type ActivityFunction<TInput = any, TOutput = any> = (ctx: ActivityContext, input: TInput) => Promise<TOutput>;
export declare class Activity {
    private static activities;
    /**
     * Register an activity function
     */
    static register<TInput = any, TOutput = any>(name: string, fn: ActivityFunction<TInput, TOutput>): void;
    /**
     * Get a registered activity function
     */
    static get(name: string): ActivityFunction | undefined;
    /**
     * Check if an activity is registered
     */
    static has(name: string): boolean;
}
/**
 * Define an activity
 */
export declare function defineActivity<TInput = any, TOutput = any>(name: string, fn: ActivityFunction<TInput, TOutput>): void;
/**
 * Activity context helpers
 */
export declare class ActivityHelpers {
    /**
     * Record a heartbeat
     */
    static heartbeat(details?: any): void;
    /**
     * Get activity info
     */
    static getInfo(): ActivityContext;
}
