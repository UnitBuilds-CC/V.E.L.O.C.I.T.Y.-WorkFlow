"use strict";
/**
 * Activity definition API
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.ActivityHelpers = exports.Activity = void 0;
exports.defineActivity = defineActivity;
class Activity {
    /**
     * Register an activity function
     */
    static register(name, fn) {
        Activity.activities.set(name, fn);
    }
    /**
     * Get a registered activity function
     */
    static get(name) {
        return Activity.activities.get(name);
    }
    /**
     * Check if an activity is registered
     */
    static has(name) {
        return Activity.activities.has(name);
    }
}
exports.Activity = Activity;
Activity.activities = new Map();
/**
 * Define an activity
 */
function defineActivity(name, fn) {
    Activity.register(name, fn);
}
/**
 * Activity context helpers
 */
class ActivityHelpers {
    /**
     * Record a heartbeat
     */
    static heartbeat(details) {
        // In a real implementation, this would send a heartbeat to the server
        console.log('Activity heartbeat:', details);
    }
    /**
     * Get activity info
     */
    static getInfo() {
        // In a real implementation, this would return the current activity context
        throw new Error('Activity context not available');
    }
}
exports.ActivityHelpers = ActivityHelpers;
//# sourceMappingURL=activity.js.map