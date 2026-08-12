"use strict";
/**
 * V.E.L.O.C.I.T.Y.-WorkFlow TypeScript SDK
 *
 * Hardware-native zero-allocation durable execution engine
 * Temporal alternative with superior performance
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Connection = exports.defineActivity = exports.Activity = exports.defineWorkflow = exports.Workflow = exports.Worker = exports.Client = void 0;
var client_1 = require("./client");
Object.defineProperty(exports, "Client", { enumerable: true, get: function () { return client_1.Client; } });
var worker_1 = require("./worker");
Object.defineProperty(exports, "Worker", { enumerable: true, get: function () { return worker_1.Worker; } });
var workflow_1 = require("./workflow");
Object.defineProperty(exports, "Workflow", { enumerable: true, get: function () { return workflow_1.Workflow; } });
Object.defineProperty(exports, "defineWorkflow", { enumerable: true, get: function () { return workflow_1.defineWorkflow; } });
var activity_1 = require("./activity");
Object.defineProperty(exports, "Activity", { enumerable: true, get: function () { return activity_1.Activity; } });
Object.defineProperty(exports, "defineActivity", { enumerable: true, get: function () { return activity_1.defineActivity; } });
var connection_1 = require("./connection");
Object.defineProperty(exports, "Connection", { enumerable: true, get: function () { return connection_1.Connection; } });
__exportStar(require("./types"), exports);
//# sourceMappingURL=index.js.map