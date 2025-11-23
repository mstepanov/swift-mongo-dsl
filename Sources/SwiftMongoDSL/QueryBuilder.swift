import Foundation
import MongoSwift
import SwiftBSON

/// A fluent DSL for building MongoDB queries with more readable syntax
public struct QueryBuilder {
    private var filter: BSONDocument = [:]
    internal var options: FindOptions = FindOptions()
    
    internal init() {}
    
    // MARK: - Field-based Filters
    
    /// Equal to condition
    @discardableResult
    public func `where`(_ field: String, equals value: BSON) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = value
        return newBuilder
    }
    
    /// Equal to condition (explicit $eq operator)
    @discardableResult
    public func `where`(_ field: String, equalsExplicit value: BSON) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$eq": value]
        return newBuilder
    }
    
    /// Equal to condition (with automatic conversion to BSON)
    @discardableResult
    public func `where`(_ field: String, equals value: Int) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = .int64(Int64(value))
        return newBuilder
    }
    
    /// Equal to condition (with automatic conversion to BSON)
    @discardableResult
    public func `where`(_ field: String, equals value: String) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = .string(value)
        return newBuilder
    }
    
    /// Equal to condition (with automatic conversion to BSON)
    @discardableResult
    public func `where`(_ field: String, equals value: Bool) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = .bool(value)
        return newBuilder
    }
    
    /// Equal to condition (with automatic conversion to BSON)
    @discardableResult
    public func `where`(_ field: String, equals value: Double) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = .double(value)
        return newBuilder
    }
    
    /// Equal to condition (with automatic conversion to BSON)
    @discardableResult
    public func `where`(_ field: String, equals value: Date) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = .datetime(value)
        return newBuilder
    }
    
    /// Not equal to condition
    @discardableResult
    public func `where`(_ field: String, notEquals value: BSON) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$ne": value]
        return newBuilder
    }
    
    /// Greater than condition
    @discardableResult
    public func `where`(_ field: String, greaterThan value: BSON) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$gt": value]
        return newBuilder
    }
    
    /// Greater than or equal to condition
    @discardableResult
    public func `where`(_ field: String, greaterThanOrEqual value: BSON) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$gte": value]
        return newBuilder
    }
    
    /// Less than condition
    @discardableResult
    public func `where`(_ field: String, lessThan value: BSON) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$lt": value]
        return newBuilder
    }
    
    /// Less than or equal to condition
    @discardableResult
    public func `where`(_ field: String, lessThanOrEqual value: BSON) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$lte": value]
        return newBuilder
    }
    
    /// In array condition
    @discardableResult
    public func `where`(_ field: String, in values: [BSON]) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$in": .array(values)]
        return newBuilder
    }
    
    /// Not in array condition
    @discardableResult
    public func `where`(_ field: String, notIn values: [BSON]) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$nin": .array(values)]
        return newBuilder
    }
    
    /// Exists condition
    @discardableResult
    public func `where`(_ field: String, exists: Bool = true) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$exists": .bool(exists)]
        return newBuilder
    }
    
    /// Type condition
    @discardableResult
    public func `where`(_ field: String, type: BSONType) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$type": .string(type.rawValue)]
        return newBuilder
    }
    
    /// Regular expression condition
    @discardableResult
    public func `where`(_ field: String, matches regex: String, options: String = "") -> QueryBuilder {
        var newBuilder = self
        if options.isEmpty {
            newBuilder.filter[field] = ["$regex": .string(regex)]
        } else {
            newBuilder.filter[field] = [
                "$regex": .string(regex),
                "$options": .string(options)
            ]
        }
        return newBuilder
    }
    
    // MARK: - Array Operations
    
    /// Array contains condition
    @discardableResult
    public func `where`(_ field: String, contains value: BSON) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = value
        return newBuilder
    }
    
    /// Array contains all condition
    @discardableResult
    public func `where`(_ field: String, containsAll values: [BSON]) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$all": .array(values)]
        return newBuilder
    }
    
    /// Array size condition
    @discardableResult
    public func `where`(_ field: String, size count: Int) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$size": .int32(Int32(count))]
        return newBuilder
    }
    
    // MARK: - Nested Field Operations
    
    /// Nested field condition using dot notation
    @discardableResult
    public func `where`(_ fieldPath: String, nestedEquals value: BSON) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[fieldPath] = value
        return newBuilder
    }
    
    /// Nested field condition with operator using dot notation
    @discardableResult
    public func `where`(_ fieldPath: String, nestedOperator op: String, value: BSON) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[fieldPath] = [op: value]
        return newBuilder
    }
    
    // MARK: - Logical Operations
    
    /// AND multiple conditions
    @discardableResult
    public func `and`(_ conditions: [BSONDocument]) -> QueryBuilder {
        var newBuilder = self
        if newBuilder.filter.keys.contains("$and") {
            var existingAnd = newBuilder.filter["$and"]!.arrayValue!
            for condition in conditions {
                existingAnd.append(BSON.document(condition))
            }
            newBuilder.filter["$and"] = .array(existingAnd)
        } else {
            newBuilder.filter["$and"] = .array(conditions.map { BSON.document($0) })
        }
        return newBuilder
    }
    
    /// OR multiple conditions
    @discardableResult
    public func `or`(_ conditions: [BSONDocument]) -> QueryBuilder {
        var newBuilder = self
        if newBuilder.filter.keys.contains("$or") {
            var existingOr = newBuilder.filter["$or"]!.arrayValue!
            for condition in conditions {
                existingOr.append(BSON.document(condition))
            }
            newBuilder.filter["$or"] = .array(existingOr)
        } else {
            newBuilder.filter["$or"] = .array(conditions.map { BSON.document($0) })
        }
        return newBuilder
    }
    
    /// NOT condition
    @discardableResult
    public func `not`(_ condition: BSONDocument) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter["$not"] = .document(condition)
        return newBuilder
    }
    
    /// NOR condition (neither condition is true)
    @discardableResult
    public func nor(_ conditions: [BSONDocument]) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter["$nor"] = .array(conditions.map { BSON.document($0) })
        return newBuilder
    }
    
    /// Modulo operation
    @discardableResult
    public func `where`(_ field: String, modulo: (divisor: Int, remainder: Int)) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$mod": .array([.int64(Int64(modulo.divisor)), .int64(Int64(modulo.remainder))])]
        return newBuilder
    }
    
    /// Expression operator (use aggregation expressions)
    @discardableResult
    public func `where`(_ field: String, expr: BSONDocument) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter["$expr"] = .document(expr)
        return newBuilder
    }
    
    /// Text search
    @discardableResult
    public func textSearch(_ search: String, language: String? = nil, caseSensitive: Bool = false, diacriticSensitive: Bool = false) -> QueryBuilder {
        var newBuilder = self
        var textQuery: BSONDocument = ["$search": .string(search)]
        if let lang = language {
            textQuery["$language"] = .string(lang)
        }
        textQuery["$caseSensitive"] = .bool(caseSensitive)
        textQuery["$diacriticSensitive"] = .bool(diacriticSensitive)
        newBuilder.filter["$text"] = .document(textQuery)
        return newBuilder
    }
    
    /// Geospatial near query
    @discardableResult
    public func `where`(_ field: String, near coordinates: [Double], maxDistance: Double? = nil) -> QueryBuilder {
        var newBuilder = self
        var nearQuery: BSONDocument = [
            "$geometry": [
                "type": "Point",
                "coordinates": .array(coordinates.map { .double($0) })
            ]
        ]
        if let maxDist = maxDistance {
            nearQuery["$maxDistance"] = .double(maxDist)
        }
        newBuilder.filter[field] = ["$near": .document(nearQuery)]
        return newBuilder
    }
    
    /// Geospatial nearSphere query
    @discardableResult
    public func `where`(_ field: String, nearSphere coordinates: [Double], maxDistance: Double? = nil) -> QueryBuilder {
        var newBuilder = self
        var nearQuery: BSONDocument = [
            "$geometry": [
                "type": "Point",
                "coordinates": .array(coordinates.map { .double($0) })
            ]
        ]
        if let maxDist = maxDistance {
            nearQuery["$maxDistance"] = .double(maxDist)
        }
        newBuilder.filter[field] = ["$nearSphere": .document(nearQuery)]
        return newBuilder
    }
    
    /// Array element match
    @discardableResult
    public func `where`(_ field: String, elemMatch condition: BSONDocument) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$elemMatch": .document(condition)]
        return newBuilder
    }
    
    /// All array elements match condition
    @discardableResult
    public func `where`(_ field: String, all values: [BSON]) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$all": .array(values)]
        return newBuilder
    }
    
    // Array size condition is already implemented as `where(_ field: String, size count: Int)` at line 171
    
    /// Type check with BSON type number
    @discardableResult
    public func `where`(_ field: String, type bsonTypeNumber: Int) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$type": .int32(Int32(bsonTypeNumber))]
        return newBuilder
    }
    
    /// Field is null
    @discardableResult
    public func `where`(_ field: String, isNull: Bool = true) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = isNull ? .null : ["$ne": .null]
        return newBuilder
    }
    
    /// Field is missing
    @discardableResult
    public func `where`(_ field: String, missing: Bool = true) -> QueryBuilder {
        var newBuilder = self
        newBuilder.filter[field] = ["$exists": .bool(!missing)]
        return newBuilder
    }
    
    /// Array slice projection (for use in aggregation)
    @discardableResult
    public func slice(_ field: String, count: Int) -> QueryBuilder {
        var newBuilder = self
        if newBuilder.options.projection == nil {
            newBuilder.options.projection = [:]
        }
        newBuilder.options.projection![field] = ["$slice": .int32(Int32(count))]
        return newBuilder
    }
    
    /// Array slice projection with skip and limit
    @discardableResult
    public func slice(_ field: String, skip: Int, limit: Int) -> QueryBuilder {
        var newBuilder = self
        if newBuilder.options.projection == nil {
            newBuilder.options.projection = [:]
        }
        newBuilder.options.projection![field] = ["$slice": .array([.int32(Int32(skip)), .int32(Int32(limit))])]
        return newBuilder
    }
    
    // MARK: - Query Options
    
    /// Sort results
    @discardableResult
    public func sort(_ field: String, direction: SortOrder = .ascending) -> QueryBuilder {
        var newBuilder = self
        newBuilder.options.sort = [field: .int32(direction.rawValue)]
        return newBuilder
    }
    
    /// Sort by multiple fields
    @discardableResult
    public func sort(_ sortFields: [(String, SortOrder)]) -> QueryBuilder {
        var newBuilder = self
        var sortDoc = BSONDocument()
        for (field, order) in sortFields {
            sortDoc[field] = .int32(order.rawValue)
        }
        newBuilder.options.sort = sortDoc
        return newBuilder
    }
    
    /// Limit number of results
    @discardableResult
    public func limit(_ count: Int) -> QueryBuilder {
        var newBuilder = self
        newBuilder.options.limit = count
        return newBuilder
    }
    
    /// Skip number of results
    @discardableResult
    public func skip(_ count: Int) -> QueryBuilder {
        var newBuilder = self
        newBuilder.options.skip = count
        return newBuilder
    }
    
    /// Select specific fields to return
    @discardableResult
    public func select(_ fields: String...) -> QueryBuilder {
        return select(fields)
    }
    
    /// Select specific fields to return
    @discardableResult
    public func select(_ fields: [String]) -> QueryBuilder {
        var newBuilder = self
        var projection = BSONDocument()
        for field in fields {
            projection[field] = .int32(1)
        }
        newBuilder.options.projection = projection
        return newBuilder
    }
    
    /// Exclude specific fields from return
    @discardableResult
    public func exclude(_ fields: String...) -> QueryBuilder {
        return exclude(fields)
    }
    
    /// Exclude specific fields from return
    @discardableResult
    public func exclude(_ fields: [String]) -> QueryBuilder {
        var newBuilder = self
        var projection = BSONDocument()
        for field in fields {
            projection[field] = .int32(0)
        }
        newBuilder.options.projection = projection
        return newBuilder
    }
    
    // MARK: - Execution
    
    /// Execute the query on the given collection
    public func find(in collection: MongoCollection<BSONDocument>) async throws -> MongoCursor<BSONDocument> {
        return try await collection.find(filter, options: options)
    }
    
    /// Execute the query and return the first result
    public func findOne(in collection: MongoCollection<BSONDocument>) async throws -> BSONDocument? {
        return try await collection.findOne(filter)
    }
    
    /// Execute the query and count the results
    public func count(in collection: MongoCollection<BSONDocument>) async throws -> Int {
        return try await collection.countDocuments(filter)
    }
    
    /// Get the underlying filter document
    public var document: BSONDocument {
        return filter
    }
}
