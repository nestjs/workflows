/**
 * The dispatcher context `@nestjs/workflows/cqrs` reads from `eventBus.publish(event, context)`,
 * `publishAll(events, context)` and, with `@nestjs/cqrs` 12.1 or later, an aggregate's
 * `commit(context)`: `transaction` is your ORM's transaction (Drizzle's `tx`, a TypeORM
 * `EntityManager`, a Prisma transaction client, a Sequelize `transaction`), so the workflows
 * the events start and the signals they send commit with your writes, or not at all.
 */
export interface WorkflowDispatcherContext {
  transaction: unknown;
}
