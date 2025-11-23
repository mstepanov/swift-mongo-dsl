import Foundation
import MongoSwift
import SwiftBSON


// MARK: - Sort Order

@frozen
public enum SortOrder: Int32 {
    case ascending = 1
    case descending = -1
}

// MARK: - BSON Type

@frozen
public enum BSONType: String {
    case double = "double"
    case string = "string"
    case object = "object"
    case array = "array"
    case bool = "bool"
    case date = "date"
    case null = "null"
    case regex = "regex"
    case int = "int"
    case timestamp = "timestamp"
    case long = "long"
    case decimal = "decimal"
    case minKey = "minKey"
    case maxKey = "maxKey"
}

// MARK: - Collection Extensions

public extension MongoCollection where T == BSONDocument {
    
    /// Create a query with builder closure
    static func query(_ build: (QueryBuilder) -> QueryBuilder) -> BSONDocument {
        return build(QueryBuilder()).document
    }
    
    /// Create an update with builder closure
    static func update(_ build: (UpdateBuilder) -> UpdateBuilder) -> BSONDocument {
        return build(UpdateBuilder()).document
    }
    
    /// Create a delete with builder closure
    static func delete(_ build: (DeleteBuilder) -> DeleteBuilder) -> BSONDocument {
        return build(DeleteBuilder()).document
    }
    
    /// Create an aggregation with builder closure
    static func aggregate(_ build: (AggregationBuilder) -> AggregationBuilder) -> [BSONDocument] {
        return build(AggregationBuilder()).pipelineArray
    }
}

// MARK: - BSONDocument Extensions

public extension BSONDocument {
    
    /// Create a query with builder closure
    static func query(_ build: (QueryBuilder) throws -> QueryBuilder) rethrows -> BSONDocument {
        return try build(QueryBuilder()).document
    }
    
    /// Create an update with builder closure
    static func update(_ build: (UpdateBuilder) throws -> UpdateBuilder) rethrows -> BSONDocument {
        return try build(UpdateBuilder()).document
    }
    
    /// Create a delete with builder closure
    static func delete(_ build: (DeleteBuilder) throws -> DeleteBuilder) rethrows -> BSONDocument {
        return try build(DeleteBuilder()).document
    }
    
    /// Create an aggregation with builder closure
    static func aggregate(_ build: (AggregationBuilder) throws -> AggregationBuilder) rethrows -> [BSONDocument] {
        return try build(AggregationBuilder()).pipelineArray
    }
}
