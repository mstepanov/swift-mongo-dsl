# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Build/Lint/Test Commands

- Build: `swift build`
- Test: `swift test`
- Run specific test: `swift test --filter TestName`
- Test with verbose output: `swift test --verbose`

## Code Style Guidelines

- DSL Usage: Use method chaining with closures like `.query { query in ... }`, `.update { update in ... }`, etc.
- Type Conversion: Automatic conversion from Swift types (Int, String, Bool, Double, Date) to BSON
- Immutable Builders: Each method returns a new builder instance, enabling method chaining
- Field Names: Use dot notation for nested fields (e.g., `"address.city"`)

## Non-Obvious Patterns

- Query syntax: Use `BSONDocument.query { ... }` or `MongoCollection.query { ... }` to create query documents
- Update syntax: Use `BSONDocument.update { ... }` to create update documents
- Aggregation syntax: Use `BSONDocument.aggregate { ... }` to create aggregation pipelines
- All builder methods are prefixed with `@discardableResult` allowing both chaining and single use
- The `where` method is implemented as a keyword to avoid conflicts with Swift's reserved word