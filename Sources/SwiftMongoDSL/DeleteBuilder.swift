import Foundation
import MongoSwift
import SwiftBSON

// MARK: - Delete DSL Builder

public struct DeleteBuilder {
    private var filter: BSONDocument = [:]
    internal var options: DeleteOptions = DeleteOptions()
    
    internal init() {}
    
    /// Add filter condition
    @discardableResult
    public func filter(_ field: String, equals value: BSON) -> DeleteBuilder {
        var newBuilder = self
        newBuilder.filter[field] = value
        return newBuilder
    }
    
    /// Add filter condition with automatic conversion for Int
    @discardableResult
    public func filter(_ field: String, equals value: Int) -> DeleteBuilder {
        var newBuilder = self
        newBuilder.filter[field] = .int64(Int64(value))
        return newBuilder
    }
    
    /// Add filter condition with automatic conversion for String
    @discardableResult
    public func filter(_ field: String, equals value: String) -> DeleteBuilder {
        var newBuilder = self
        newBuilder.filter[field] = .string(value)
        return newBuilder
    }
    
    /// Add filter condition with automatic conversion for Bool
    @discardableResult
    public func filter(_ field: String, equals value: Bool) -> DeleteBuilder {
        var newBuilder = self
        newBuilder.filter[field] = .bool(value)
        return newBuilder
    }
    
    /// Add filter condition with automatic conversion for Double
    @discardableResult
    public func filter(_ field: String, equals value: Double) -> DeleteBuilder {
        var newBuilder = self
        newBuilder.filter[field] = .double(value)
        return newBuilder
    }
    
    /// Add filter condition with automatic conversion for Date
    @discardableResult
    public func filter(_ field: String, equals value: Date) -> DeleteBuilder {
        var newBuilder = self
        newBuilder.filter[field] = .datetime(value)
        return newBuilder
    }
    
    /// Add filter condition with operator
    @discardableResult
    public func filter(_ field: String, operator op: String, value: BSON) -> DeleteBuilder {
        var newBuilder = self
        newBuilder.filter[field] = [op: value]
        return newBuilder
    }
    
    /// Add OR condition
    @discardableResult
    public func or(_ conditions: [BSONDocument]) -> DeleteBuilder {
        var newBuilder = self
        newBuilder.filter["$or"] = .array(conditions.map { BSON.document($0) })
        return newBuilder
    }
    /// Execute delete on single document
    @discardableResult
    public func deleteOne(in collection: MongoCollection<BSONDocument>) async throws -> DeleteResult {
        let result = try await collection.deleteOne(filter, options: options)
        return result!
    }
    
    /// Execute delete on multiple documents
    @discardableResult
    public func deleteMany(in collection: MongoCollection<BSONDocument>) async throws -> DeleteResult {
        let result = try await collection.deleteMany(filter, options: options)
        return result!
    }
    
    /// Get the underlying filter document
    public var document: BSONDocument {
        return filter
    }
}
