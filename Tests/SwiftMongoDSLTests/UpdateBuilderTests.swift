import Testing
@testable import SwiftMongoDSL
import MongoSwift
import SwiftBSON
import Foundation

struct UpdateBuilderTests {
    
    // MARK: - Set Operations Tests
    
    @Test func testUpdateBuilderSetWithBSONCreatesCorrectUpdate() {
        let update = UpdateBuilder().set("name", to: .string("John"))
        let expected: BSONDocument = ["$set": ["name": .string("John")]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderSetWithIntCreatesCorrectUpdate() {
        let update = UpdateBuilder().set("age", to: 25)
        let expected: BSONDocument = ["$set": ["age": .int64(25)]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderSetWithStringCreatesCorrectUpdate() {
        let update = UpdateBuilder().set("name", to: "John")
        let expected: BSONDocument = ["$set": ["name": .string("John")]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderSetWithBoolCreatesCorrectUpdate() {
        let update = UpdateBuilder().set("active", to: true)
        let expected: BSONDocument = ["$set": ["active": .bool(true)]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderSetWithDoubleCreatesCorrectUpdate() {
        let update = UpdateBuilder().set("price", to: 19.99)
        let expected: BSONDocument = ["$set": ["price": .double(19.99)]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderSetWithDateCreatesCorrectUpdate() {
        let date = Date(timeIntervalSince1970: 167886400) // March 15, 2023
        let update = UpdateBuilder().set("createdAt", to: date)
        let expected: BSONDocument = ["$set": ["createdAt": .datetime(date)]]
        #expect(update.document == expected)
    }
    
    // MARK: - Increment Operations Tests
    
    @Test func testUpdateBuilderIncrementWithIntCreatesCorrectUpdate() {
        let update = UpdateBuilder().increment("count", by: 1)
        let expected: BSONDocument = ["$inc": ["count": .int64(1)]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderIncrementWithDoubleCreatesCorrectUpdate() {
        let update = UpdateBuilder().increment("value", by: 1.5)
        let expected: BSONDocument = ["$inc": ["value": .double(1.5)]]
        #expect(update.document == expected)
    }
    
    // MARK: - Pop Operations Tests
    
    @Test func testUpdateBuilderPopLastCreatesCorrectUpdate() {
        let update = UpdateBuilder().pop("tags", first: false)
        let expected: BSONDocument = ["$pop": ["tags": .int32(1)]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderPopFirstCreatesCorrectUpdate() {
        let update = UpdateBuilder().pop("tags", first: true)
        let expected: BSONDocument = ["$pop": ["tags": .int32(-1)]]
        #expect(update.document == expected)
    }
    
    // MARK: - Add to Set Operations Tests
    
    @Test func testUpdateBuilderAddEachToSetCreatesCorrectUpdate() {
        let update = UpdateBuilder().addEachToSet("tags", values: [.string("tech"), .string("swift")])
        let expected: BSONDocument = ["$addToSet": ["tags": ["$each": .array([.string("tech"), .string("swift")])]]]
        #expect(update.document == expected)
    }
    
    // MARK: - Push Operations Tests
    
    @Test func testUpdateBuilderPushWithBSONCreatesCorrectUpdate() {
        let update = UpdateBuilder().push("items", value: .string("item1"))
        let expected: BSONDocument = ["$push": ["items": .string("item1")]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderPushWithOptionsCreatesCorrectUpdate() {
        let options: [(String, BSON)] = [("slice", .int32(5))]
        let update = UpdateBuilder().push("comments", value: .string("Great post!"), options: options)
        let expected: BSONDocument = [
            "$push": [
                "comments": [
                    "$each": .array([.string("Great post!")]),
                    "slice": .int32(5)
                ]
            ]
        ]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderPushEachCreatesCorrectUpdate() {
        let update = UpdateBuilder().pushEach("tags", values: [.string("tech"), .string("programming")])
        let expected: BSONDocument = [
            "$push": [
                "tags": [
                    "$each": .array([.string("tech"), .string("programming")])
                ]
            ]
        ]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderPushEachWithOptionsCreatesCorrectUpdate() {
        let options: [(String, BSON)] = [("sort", .int32(-1))]
        let update = UpdateBuilder().pushEach("scores", values: [.int64(85), .int64(90)], options: options)
        let expected: BSONDocument = [
            "$push": [
                "scores": [
                    "$each": .array([.int64(85), .int64(90)]),
                    "sort": .int32(-1)
                ]
            ]
        ]
        #expect(update.document == expected)
    }
    
    // MARK: - Pull Operations Tests
    
    @Test func testUpdateBuilderPullAllCreatesCorrectUpdate() {
        let update = UpdateBuilder().pullAll("tags", values: [.string("old"), .string("deprecated")])
        let expected: BSONDocument = ["$pullAll": ["tags": .array([.string("old"), .string("deprecated")])]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderPullCreatesCorrectUpdate() {
        let update = UpdateBuilder().pull("tags", value: .string("old"))
        let expected: BSONDocument = ["$pull": ["tags": .string("old")]]
        #expect(update.document == expected)
    }
    
    // MARK: - Positional Update Operations Tests
    
    @Test func testUpdateBuilderSetAtPositionCreatesCorrectUpdate() {
        let update = UpdateBuilder().setAtPosition("items", to: .string("updated"))
        let expected: BSONDocument = ["$set": ["items.$": .string("updated")]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderSetAtAllPositionsCreatesCorrectUpdate() {
        let update = UpdateBuilder().setAtAllPositions("items", to: .string("updated"))
        let expected: BSONDocument = ["$set": ["items.$[]": .string("updated")]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderSetAtFilteredPositionCreatesCorrectUpdate() {
        let condition: BSONDocument = ["status": .string("active")]
        let update = UpdateBuilder().setAtFilteredPosition("items", to: .string("updated"), identifier: "item", condition: condition)
        let expected: BSONDocument = ["$set": ["items.$[.item]": .string("updated")]]
        #expect(update.document == expected)
        #expect(update.options.arrayFilters == [condition])
    }
    
    // MARK: - Multiply Operations Tests
    
    @Test func testUpdateBuilderMultiplyCreatesCorrectUpdate() {
        let update = UpdateBuilder().multiply("price", by: 1.1)
        let expected: BSONDocument = ["$mul": ["price": .double(1.1)]]
        #expect(update.document == expected)
    }
    
    // MARK: - SetOnInsert Operations Tests
    
    @Test func testUpdateBuilderSetOnInsertCreatesCorrectUpdate() {
        let update = UpdateBuilder().setOnInsert("createdAt", to: .datetime(Date()))
        let expected: BSONDocument = ["$setOnInsert": ["createdAt": .datetime(Date())]]
        #expect(update.document.keys.contains("$setOnInsert"))
        #expect(update.document["$setOnInsert"]?.documentValue?.keys.contains("createdAt") == true)
    }
    
    // MARK: - Rename Operations Tests
    
    @Test func testUpdateBuilderRenameCreatesCorrectUpdate() {
        let update = UpdateBuilder().rename("oldName", to: "newName")
        let expected: BSONDocument = ["$rename": ["oldName": .string("newName")]]
        #expect(update.document == expected)
    }
    
    // MARK: - Unset Operations Tests
    
    @Test func testUpdateBuilderUnsetCreatesCorrectUpdate() {
        let update = UpdateBuilder().unset("oldField")
        let expected: BSONDocument = ["$unset": ["oldField": .int32(1)]]
        #expect(update.document == expected)
    }
    
    // MARK: - CurrentDate Operations Tests
    
    @Test func testUpdateBuilderCurrentDateCreatesCorrectUpdate() {
        let update = UpdateBuilder().currentDate("updatedAt")
        let expected: BSONDocument = ["$currentDate": ["updatedAt": .bool(true)]]
        #expect(update.document == expected)
    }
    
    // MARK: - AddToSet Operations Tests
    
    @Test func testUpdateBuilderAddToSetCreatesCorrectUpdate() {
        let update = UpdateBuilder().addToSet("tags", value: .string("tech"))
        let expected: BSONDocument = ["$addToSet": ["tags": .string("tech")]]
        #expect(update.document == expected)
    }
    
    // MARK: - Upsert Options Tests
    
    @Test func testUpdateBuilderUpsertCreatesCorrectOptions() {
        let update = UpdateBuilder().upsert(true)
        #expect(update.options.upsert == true)
    }
    
    @Test func testUpdateBuilderUpsertFalseCreatesCorrectOptions() {
        let update = UpdateBuilder().upsert(false)
        #expect(update.options.upsert == false)
    }
    
    // MARK: - Method Chaining Tests
    
    @Test func testUpdateBuilderMethodChaining() {
        let update = UpdateBuilder()
            .set("name", to: "John")
            .set("age", to: 30)
            .increment("count", by: 1)
        
        let expectedSet: BSONDocument = ["name": .string("John"), "age": .int64(30)]
        let expectedInc: BSONDocument = ["count": .int64(1)]
        
        #expect(update.document["$set"]?.documentValue == expectedSet)
        #expect(update.document["$inc"]?.documentValue == expectedInc)
    }
    
    // MARK: - Builder Immutability Tests
    
    @Test func testUpdateBuilderImmutability() {
        let initialUpdate = UpdateBuilder().set("status", to: "active")
        let update1 = initialUpdate.set("name", to: "John")
        let update2 = initialUpdate.set("type", to: "premium")
        
        let expectedInitial: BSONDocument = ["$set": ["status": .string("active")]]
        let expectedUpdate1: BSONDocument = ["$set": ["status": .string("active"), "name": .string("John")]]
        let expectedUpdate2: BSONDocument = ["$set": ["status": .string("active"), "type": .string("premium")]]
        
        #expect(initialUpdate.document == expectedInitial)
        #expect(update1.document == expectedUpdate1)
        #expect(update2.document == expectedUpdate2)
    }
    
    // MARK: - Edge Cases Tests
    
    @Test func testUpdateBuilderSetWithEmptyString() {
        let update = UpdateBuilder().set("name", to: "")
        let expected: BSONDocument = ["$set": ["name": .string("")]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderSetWithZero() {
        let update = UpdateBuilder().set("count", to: 0)
        let expected: BSONDocument = ["$set": ["count": .int64(0)]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderSetWithMaxInt() {
        let update = UpdateBuilder().set("value", to: Int.max)
        let expected: BSONDocument = ["$set": ["value": .int64(Int64(Int.max))]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderSetWithMinInt() {
        let update = UpdateBuilder().set("value", to: Int.min)
        let expected: BSONDocument = ["$set": ["value": .int64(Int64(Int.min))]]
        #expect(update.document == expected)
    }
    
    @Test func testUpdateBuilderSetWithSpecialBSONValues() {
        let update = UpdateBuilder().set("field", to: .null)
        let expected: BSONDocument = ["$set": ["field": .null]]
        #expect(update.document == expected)
    }
}