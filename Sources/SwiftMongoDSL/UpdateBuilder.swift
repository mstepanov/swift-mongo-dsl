import Foundation
import MongoSwift
import SwiftBSON

public struct UpdateBuilder {
    private var updates: BSONDocument = [:]
    internal var options: UpdateOptions = UpdateOptions()
    
    internal init() {}
    
    // MARK: - Update Operators

    /// Set field value
    @discardableResult
    public func set(_ value: Codable) throws -> UpdateBuilder {
        var newBuilder = self
        newBuilder.updates["$set"] = try .document(BSONEncoder().encode(value))
        return newBuilder
    }

    /// Set field value
    @discardableResult
    public func set(_ value: BSONDocument) -> UpdateBuilder {
        var newBuilder = self
        newBuilder.updates["$set"] = .document(value)
        return newBuilder
    }

    /// Set field value
    @discardableResult
    public func set(_ field: String, to value: BSON) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$set"] == nil {
            newBuilder.updates["$set"] = .document(BSONDocument())
        }
        var setDoc = newBuilder.updates["$set"]!.documentValue!
        setDoc[field] = value
        newBuilder.updates["$set"] = .document(setDoc)
        return newBuilder
    }
    
    /// Set field value with automatic conversion for Int
    @discardableResult
    public func set(_ field: String, to value: Int) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$set"] == nil {
            newBuilder.updates["$set"] = .document(BSONDocument())
        }
        var setDoc = newBuilder.updates["$set"]!.documentValue!
        setDoc[field] = .int64(Int64(value))
        newBuilder.updates["$set"] = .document(setDoc)
        return newBuilder
    }
    
    /// Set field value with automatic conversion for String
    @discardableResult
    public func set(_ field: String, to value: String) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$set"] == nil {
            newBuilder.updates["$set"] = .document(BSONDocument())
        }
        var setDoc = newBuilder.updates["$set"]!.documentValue!
        setDoc[field] = .string(value)
        newBuilder.updates["$set"] = .document(setDoc)
        return newBuilder
    }
    
    /// Set field value with automatic conversion for Bool
    @discardableResult
    public func set(_ field: String, to value: Bool) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$set"] == nil {
            newBuilder.updates["$set"] = .document(BSONDocument())
        }
        var setDoc = newBuilder.updates["$set"]!.documentValue!
        setDoc[field] = .bool(value)
        newBuilder.updates["$set"] = .document(setDoc)
        return newBuilder
    }
    
    /// Set field value with automatic conversion for Double
    @discardableResult
    public func set(_ field: String, to value: Double) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$set"] == nil {
            newBuilder.updates["$set"] = .document(BSONDocument())
        }
        var setDoc = newBuilder.updates["$set"]!.documentValue!
        setDoc[field] = .double(value)
        newBuilder.updates["$set"] = .document(setDoc)
        return newBuilder
    }
    
    /// Set field value with automatic conversion for Date
    @discardableResult
    public func set(_ field: String, to value: Date) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$set"] == nil {
            newBuilder.updates["$set"] = .document(BSONDocument())
        }
        var setDoc = newBuilder.updates["$set"]!.documentValue!
        setDoc[field] = .datetime(value)
        newBuilder.updates["$set"] = .document(setDoc)
        return newBuilder
    }
    
    /// Increment field value
    @discardableResult
    public func increment(_ field: String, by value: Int) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$inc"] == nil {
            newBuilder.updates["$inc"] = .document(BSONDocument())
        }
        var incDoc = newBuilder.updates["$inc"]!.documentValue!
        incDoc[field] = .int64(Int64(value))
        newBuilder.updates["$inc"] = .document(incDoc)
        return newBuilder
    }
    
    /// Increment field value with automatic conversion for Double
    @discardableResult
    public func increment(_ field: String, by value: Double) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$inc"] == nil {
            newBuilder.updates["$inc"] = .document(BSONDocument())
        }
        var incDoc = newBuilder.updates["$inc"]!.documentValue!
        incDoc[field] = .double(value)
        newBuilder.updates["$inc"] = .document(incDoc)
        return newBuilder
    }
    
    /// Pop value from array (remove first or last element)
    @discardableResult
    public func pop(_ field: String, first: Bool = false) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$pop"] == nil {
            newBuilder.updates["$pop"] = .document(BSONDocument())
        }
        var popDoc = newBuilder.updates["$pop"]!.documentValue!
        popDoc[field] = .int32(first ? -1 : 1)  // -1 for first, 1 for last
        newBuilder.updates["$pop"] = .document(popDoc)
        return newBuilder
    }
    
    /// Add each value from array to set (array without duplicates)
    @discardableResult
    public func addEachToSet(_ field: String, values: [BSON]) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$addToSet"] == nil {
            newBuilder.updates["$addToSet"] = .document(BSONDocument())
        }
        var addToSetDoc = newBuilder.updates["$addToSet"]!.documentValue!
        addToSetDoc[field] = ["$each": .array(values)]
        newBuilder.updates["$addToSet"] = .document(addToSetDoc)
        return newBuilder
    }
    
    /// Push value to array with options
    @discardableResult
    public func push(_ field: String, value: BSON, options: [(String, BSON)] = []) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$push"] == nil {
            newBuilder.updates["$push"] = .document(BSONDocument())
        }
        var pushDoc = newBuilder.updates["$push"]!.documentValue!
        if options.isEmpty {
            pushDoc[field] = value
        } else {
            var pushValue: BSONDocument = [:]
            pushValue["$each"] = .array([value])
            for (option, optionValue) in options {
                pushValue[option] = optionValue
            }
            pushDoc[field] = .document(pushValue)
        }
        newBuilder.updates["$push"] = .document(pushDoc)
        return newBuilder
    }
    
    
    /// Push each value from array to array with options
    @discardableResult
    public func pushEach(_ field: String, values: [BSON], options: [(String, BSON)] = []) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$push"] == nil {
            newBuilder.updates["$push"] = .document(BSONDocument())
        }
        var pushDoc = newBuilder.updates["$push"]!.documentValue!
        var pushValue: BSONDocument = [:]
        pushValue["$each"] = .array(values)
        for (option, optionValue) in options {
            pushValue[option] = optionValue
        }
        pushDoc[field] = .document(pushValue)
        newBuilder.updates["$push"] = .document(pushDoc)
        return newBuilder
    }
    
    
    /// Pull all values matching condition from array
    @discardableResult
    public func pullAll(_ field: String, values: [BSON]) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$pullAll"] == nil {
            newBuilder.updates["$pullAll"] = .document(BSONDocument())
        }
        var pullAllDoc = newBuilder.updates["$pullAll"]!.documentValue!
        pullAllDoc[field] = .array(values)
        newBuilder.updates["$pullAll"] = .document(pullAllDoc)
        return newBuilder
    }
    
    /// Positional operator to update array element matching query condition
    @discardableResult
    public func setAtPosition(_ field: String, to value: BSON) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$set"] == nil {
            newBuilder.updates["$set"] = .document(BSONDocument())
        }
        var setDoc = newBuilder.updates["$set"]!.documentValue!
        setDoc["\(field).$"] = value
        newBuilder.updates["$set"] = .document(setDoc)
        return newBuilder
    }
    
    /// Update all array elements
    @discardableResult
    public func setAtAllPositions(_ field: String, to value: BSON) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$set"] == nil {
            newBuilder.updates["$set"] = .document(BSONDocument())
        }
        var setDoc = newBuilder.updates["$set"]!.documentValue!
        setDoc["\(field).$[]"] = value
        newBuilder.updates["$set"] = .document(setDoc)
        return newBuilder
    }
    
    /// Update filtered array elements
    @discardableResult
    public func setAtFilteredPosition(_ field: String, to value: BSON, identifier: String, condition: BSONDocument) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$set"] == nil {
            newBuilder.updates["$set"] = .document(BSONDocument())
        }
        var setDoc = newBuilder.updates["$set"]!.documentValue!
        setDoc["\(field).$[.\(identifier)]"] = value
        newBuilder.updates["$set"] = .document(setDoc)
        newBuilder.options.arrayFilters = [condition]
        return newBuilder
    }
    
    /// Multiply field value
    @discardableResult
    public func multiply(_ field: String, by value: Double) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$mul"] == nil {
            newBuilder.updates["$mul"] = .document(BSONDocument())
        }
        var mulDoc = newBuilder.updates["$mul"]!.documentValue!
        mulDoc[field] = .double(value)
        newBuilder.updates["$mul"] = .document(mulDoc)
        return newBuilder
    }
    
    /// Add field if it doesn't exist
    @discardableResult
    public func setOnInsert(_ field: String, to value: BSON) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$setOnInsert"] == nil {
            newBuilder.updates["$setOnInsert"] = .document(BSONDocument())
        }
        var setOnInsertDoc = newBuilder.updates["$setOnInsert"]!.documentValue!
        setOnInsertDoc[field] = value
        newBuilder.updates["$setOnInsert"] = .document(setOnInsertDoc)
        return newBuilder
    }
    
    /// Rename field
    @discardableResult
    public func rename(_ oldField: String, to newField: String) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$rename"] == nil {
            newBuilder.updates["$rename"] = .document(BSONDocument())
        }
        var renameDoc = newBuilder.updates["$rename"]!.documentValue!
        renameDoc[oldField] = .string(newField)
        newBuilder.updates["$rename"] = .document(renameDoc)
        return newBuilder
    }
    
    /// Unset field (remove it)
    @discardableResult
    public func unset(_ field: String) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$unset"] == nil {
            newBuilder.updates["$unset"] = .document(BSONDocument())
        }
        var unsetDoc = newBuilder.updates["$unset"]!.documentValue!
        unsetDoc[field] = .int32(1)  // 1 is the value to remove the field
        newBuilder.updates["$unset"] = .document(unsetDoc)
        return newBuilder
    }
    
    /// Current date for field
    @discardableResult
    public func currentDate(_ field: String) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$currentDate"] == nil {
            newBuilder.updates["$currentDate"] = .document(BSONDocument())
        }
        var currentDateDoc = newBuilder.updates["$currentDate"]!.documentValue!
        currentDateDoc[field] = .bool(true)
        newBuilder.updates["$currentDate"] = .document(currentDateDoc)
        return newBuilder
    }
    
    // MARK: - Array Operators
    
    /// Push value to array
    @discardableResult
    public func push(_ field: String, value: BSON) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$push"] == nil {
            newBuilder.updates["$push"] = .document(BSONDocument())
        }
        var pushDoc = newBuilder.updates["$push"]!.documentValue!
        pushDoc[field] = value
        newBuilder.updates["$push"] = .document(pushDoc)
        return newBuilder
    }
    
    /// Add value to set (array without duplicates)
    @discardableResult
    public func addToSet(_ field: String, value: BSON) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$addToSet"] == nil {
            newBuilder.updates["$addToSet"] = .document(BSONDocument())
        }
        var addToSetDoc = newBuilder.updates["$addToSet"]!.documentValue!
        addToSetDoc[field] = value
        newBuilder.updates["$addToSet"] = .document(addToSetDoc)
        return newBuilder
    }
    
    /// Pull value from array
    @discardableResult
    public func pull(_ field: String, value: BSON) -> UpdateBuilder {
        var newBuilder = self
        if newBuilder.updates["$pull"] == nil {
            newBuilder.updates["$pull"] = .document(BSONDocument())
        }
        var pullDoc = newBuilder.updates["$pull"]!.documentValue!
        pullDoc[field] = value
        newBuilder.updates["$pull"] = .document(pullDoc)
        return newBuilder
    }
    
    // MARK: - Options
    
    /// Set upsert option
    @discardableResult
    public func upsert(_ shouldUpsert: Bool = true) -> UpdateBuilder {
        var newBuilder = self
        newBuilder.options.upsert = shouldUpsert
        return newBuilder
    }
    
    // MARK: - Execution
    
    /// Execute update on single document
    @discardableResult
    public func updateOne(in collection: MongoCollection<BSONDocument>,
                          where query: QueryBuilder) async throws -> UpdateResult {
        let result = try await collection.updateOne(filter: query.document,
                                                    update: updates,
                                                    options: options)
        return result!
    }
    
    /// Execute update on multiple documents
    @discardableResult
    public func updateMany(in collection: MongoCollection<BSONDocument>,
                           where query: QueryBuilder) async throws -> UpdateResult {
        let result = try await collection.updateMany(filter: query.document,
                                                     update: updates,
                                                     options: options)
        return result!
    }
    
    /// Get the underlying update document
    public var document: BSONDocument {
        return updates
    }
}
