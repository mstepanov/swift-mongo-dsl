# SwiftMongoDSL: A Fluent MongoDB Query DSL for Swift

## Overview

The SwiftMongoDSL provides a fluent, readable interface for building MongoDB queries in Swift. It simplifies the verbose BSONDocument syntax and makes database operations more intuitive.

## Installation

The DSL is included in the SignalScope project and works with the existing MongoSwift driver.

## Basic Usage

### Query Builder

#### Before (Traditional BSONDocument syntax)
```swift
let filter: BSONDocument = [
    "status": "A",
    "qty": ["$lt": 30]
]

let options = FindOptions(
    sort: ["item": 1],
    limit: 10,
    projection: ["item": 1, "status": 1]
)

let cursor = try await collection.find(filter, options: options)
```

#### After (Using SwiftMongoDSL)
```swift
let cursor = try await collection
    .find(.query {
        $0.where("status", equals: "A")
           .where("qty", lessThan: 30)
           .sort("item", direction: .ascending)
           .limit(10)
           .select("item", "status")
    })
```

### Update Builder

#### Before
```swift
let filter: BSONDocument = ["item": "paper"]
let update: BSONDocument = [
    "$set": [
        "size.uom": "cm",
        "status": "P"
    ],
    "$currentDate": ["lastModified": true]
]

let result = try await collection.updateOne(filter: filter, update: update)
```

#### After
```swift
let result = try await collection
    .updateOne(
        filter: .query {
            $0.where("item", equals: "paper")
        },
        update: .update {
            $0.set("size.uom", to: "cm")
               .set("status", to: "P")
               .currentDate("lastModified")
        }
    )
```

### Delete Builder

#### Before
```swift
let filter: BSONDocument = [
    "$or": [
        ["status": "A"],
        ["qty": ["$lt": 30]]
    ]
]

let result = try await collection.deleteMany(filter)
```

#### After
```swift
let result = try await collection
    .deleteMany(.query {
        $0.or([
            ["status": "A"],
            ["qty": ["$lt": 30]]
        ])
    })
```

## Query Builder Features

### Basic Field Filters

```swift
// Equality
.query { query in
    query.where("field", equals: "value")
}

// With automatic type conversion
.query { query in
    query.where("count", equals: 5)
}

.query { query in
    query.where("active", equals: true)
}

.query { query in
    query.where("price", equals: 99.99)
}

.query { query in
    query.where("createdAt", equals: Date())
}

// Comparison operators
.query { query in
    query.where("age", greaterThan: 18)
}

.query { query in
    query.where("score", greaterThanOrEqual: 90)
}

.query { query in
    query.where("temperature", lessThan: 0)
}

.query { query in
    query.where("rating", lessThanOrEqual: 5)
}

// In/Not In
.query { query in
    query.where("status", in: ["A", "B", "C"])
}

.query { query in
    query.where("category", notIn: ["deleted", "archived"])
}

// Existence
.query { query in
    query.where("email", exists: true)
}

.query { query in
    query.where("phone", exists: false)
}

// Type checking
.query { query in
    query.where("count", type: .int)
}

.query { query in
    query.where("name", type: .string)
}
```

### Array Operations

```swift
// Array contains
.query { query in
    query.where("tags", contains: "featured")
}

// Array contains all
.query { query in
    query.where("permissions", containsAll: ["read", "write"])
}

// Array size
.query { query in
    query.where("items", size: 5)
}
```

### Nested Field Operations

```swift
// Dot notation
.query { query in
    query.where("address.city", equals: "New York")
}

.query { query in
    query.where("metadata.type", equals: "document")
}
```

### Logical Operations

```swift
// AND conditions
.query { query in
    query.where("status", equals: "A")
         .where("qty", lessThan: 30)
}

// OR conditions
.query { query in
    query.or([
        ["status": "A"],
        ["qty": ["$lt": 30]]
    ])
}

// Complex combinations
.query { query in
    query.where("status", equals: "A")
         .or([
             ["qty": ["$lt": 30]],
             ["priority": ["$gt": 5]]
         ])
}
```

### Query Options

```swift
.query { query in
    query.sort("name", direction: .ascending)
         .sort("date", direction: .descending)
    // Or sort by multiple fields
    query.sort([("name", .ascending), ("date", .descending)])
    query.limit(10)
         .skip(5)
         .select("name", "email", "status")  // Only return these fields
         .exclude("password", "internal")    // Exclude these fields
}
```

## Update Builder Features

### Field Updates

```swift
.update { update in
    update.set("name", to: "New Name")
           .set("count", to: 42)
           .set("active", to: true)
           .increment("views", by: 1)
           .multiply("price", by: 1.1)  // 10% increase
           .rename("oldField", to: "newField")
           .unset("temporaryField")
           .currentDate("updatedAt")
}
```

// Array Operations

```swift
.update { update in
    update.push("tags", value: "new-tag")
           .addToSet("permissions", value: "read")
           .pull("items", value: "unwanted-item")
}
```

// Options

```swift
.update { update in
    update.upsert()  // Create if doesn't exist
           .set("name", to: "value")
}
```

## Aggregation Builder

### Basic Pipeline

```swift
let results = try await collection
    .aggregate(.aggregate { agg in
        agg.match(.query { query in
            query.where("status", equals: "A")
        })
        .group(.string("$category"), fields: [
            "total": ["$sum": "$amount"],
            "count": ["$sum": 1]
        ])
        .sort("total", direction: .descending)
        .limit(10)
    })
```

### Common Stages

```swift
.aggregate { agg in
    agg.match(filter)           // Filter documents
        .group(id, fields: [...]) // Group documents
        .project(fields)         // Reshape documents
        .sort(field, direction)  // Sort results
        .limit(count)            // Limit results
        .skip(count)             // Skip results
        .lookup(from: "otherCollection", localField: "id", foreignField: "refId", as: "joined")
        .unwind("arrayField")    // Deconstruct arrays
}
```

### Collection Extensions

The DSL provides convenient extensions on MongoCollection:

```swift
let collection = db.collection("users")

// Query
let results = try await collection
    .find(.query { query in
        query.where("active", equals: true)
    })

// Update
let updateResult = try await collection
    .updateOne(
        filter: .query { query in
            query.where("id", equals: userId)
        },
        update: .update { update in
            update.set("lastLogin", to: Date())
        }
    )

// Delete
let deleteResult = try await collection
    .deleteMany(.query { query in
        query.where("status", equals: "deleted")
    })

// Aggregate
let aggResults = try await collection
    .aggregate(.aggregate { agg in
        agg.match(.query { query in
            query.where("date", greaterThan: cutoffDate)
        })
    })
```
### BSONDocument Extensions

For complex queries that are used in multiple places:

```swift
let results = try await collection
    .find(.query { query in
        query.where("status", equals: "active")
             .where("createdDate", greaterThan: Date().addingTimeInterval(-86400)) // Last 24 hours
    })
```

## Benefits

1. **Readability**: Method chaining makes queries self-documenting
2. **Type Safety**: Automatic conversion from Swift types to BSON
3. **IntelliSense Support**: Full IDE support with method names and parameters
4. **Reduced Boilerplate**: Less verbose than raw BSONDocument syntax
5. **Error Reduction**: Compiler catches typos in field names and operators
6. **Consistency**: Uniform syntax across query, update, delete, and aggregation operations

## Migration Guide

To migrate from traditional BSONDocument syntax to SwiftMongoDSL:

1. Replace `BSONDocument` creation with `.query { query in ... }`, `.update { update in ... }`, etc.
2. Convert field-value pairs to `.where(field, operator: value)` calls inside the closure
3. Convert options to their respective method calls
4. Chain methods inside the closure instead of building separate objects

The DSL is fully compatible with the existing MongoSwift driver and can be used alongside traditional syntax during migration.