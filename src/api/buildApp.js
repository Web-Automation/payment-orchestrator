import Fastify from 'fastify';
import { registerRoutes } from './routes.js';

/**
 * Builds a Fastify instance wired to the given deps without starting the
 * health-probe loop or binding a port — used by both the real server
 * bootstrap (server.js) and API-level tests (test/api.test.js).
 */
export function buildApp(deps, fastifyOpts = { logger: false }) {
  const app = Fastify(fastifyOpts);
  registerRoutes(app, deps);
  return app;
}
