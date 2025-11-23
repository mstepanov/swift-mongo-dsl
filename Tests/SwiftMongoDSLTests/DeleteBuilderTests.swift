import Testing
@testable import SwiftMongoDSL
import MongoSwift
import SwiftBSON
import Foundation

struct DeleteBuilderTests {
    
    // MARK: - Filter Operations Tests
    
    @Test func testDeleteBuilderFilterWithBSONCreatesCorrectFilter() {
        let delete = DeleteBuilder().filter("name", equals: .string("John"))
        let expected: BSONDocument = ["name": .string("John")]
        #expect(delete.document == expected)
    }
    
    @Test func testDeleteBuilderFilterWithIntCreatesCorrectFilter() {
        let delete = DeleteBuilder().filter("age", equals: 25)
        let expected: BSONDocument = ["age": .int64(25)]
        #expect(delete.document == expected)
    }
    
    @Test func testDeleteBuilderFilterWithStringCreatesCorrectFilter() {
        let delete = DeleteBuilder().filter("name", equals: "John")
        let expected: BSONDocument = ["name": .string("John")]
        #expect(delete.document == expected)
    }
    
    @Test func testDeleteBuilderFilterWithBoolCreatesCorrectFilter() {
        let delete = DeleteBuilder().filter("active", equals: true)
        let expected: BSONDocument = ["active": .bool(true)]
        #expect(delete.document == expected)
    }
    
    @Test func testDeleteBuilderFilterWithDoubleCreatesCorrectFilter() {
        let delete = DeleteBuilder().filter("price", equals: 19.99)
        let expected: BSONDocument = ["price": .double(19.99)]
        #expect(delete.document == expected)
    }
    
    @Test func testDeleteBuilderFilterWithDateCreatesCorrectFilter() {
        let date = Date(timeIntervalSince1970: 167886400) // March 15, 2023
        let delete = DeleteBuilder().filter("createdAt", equals: date)
        let expected: BSONDocument = ["createdAt": .datetime(date)]
        #expect(delete.document == expected)
    }
    
    @Test func testDeleteBuilderFilterWithOperatorCreatesCorrectFilter() {
        let delete = DeleteBuilder().filter("age", operator: "$gt", value: .int64(18))
        let expected: BSONDocument = ["age": ["$gt": .int64(18)]]
        #expect(delete.document == expected)
    }
    
    // MARK: - Logical Operations Tests
    
    @Test func testDeleteBuilderOrCreatesCorrectFilter() {
        let condition1: BSONDocument = ["status": .string("active")]
        let condition2: BSONDocument = ["type": .string("premium")]
        let delete = DeleteBuilder().or([condition1, condition2])
        let expected: BSONDocument = ["$or": .array([.document(condition1), .document(condition2)])]
        #expect(delete.document == expected)
    }
    
    // MARK: - Method Chaining Tests
    
    @Test func testDeleteBuilderMethodChaining() {
        let condition1: BSONDocument = ["status": .string("active")]
        let condition2: BSONDocument = ["type": .string("premium")]
        
        let delete = DeleteBuilder()
            .filter("name", equals: "John")
            .or([condition1, condition2])
        
        let expectedFilter: BSONDocument = [
            "name": .string("John"),
            "$or": .array([.document(condition1), .document(condition2)])
        ]
        
        #expect(delete.document == expectedFilter)
    }
    
    // MARK: - Builder Immutability Tests
    
    @Test func testDeleteBuilderImmutability() {
        let initialDelete = DeleteBuilder().filter("status", equals: "active")
        let delete1 = initialDelete.filter("name", equals: "John")
        let delete2 = initialDelete.filter("type", equals: "premium")
        
        let expectedInitial: BSONDocument = ["status": .string("active")]
        let expectedDelete1: BSONDocument = ["status": .string("active"), "name": .string("John")]
        let expectedDelete2: BSONDocument = ["status": .string("active"), "type": .string("premium")]
        
        #expect(initialDelete.document == expectedInitial)
        #expect(delete1.document == expectedDelete1)
        #expect(delete2.document == expectedDelete2)
    }
    
    // MARK: - Edge Cases Tests
    
    @Test func testDeleteBuilderFilterWithEmptyString() {
        let delete = DeleteBuilder().filter("name", equals: "")
        let expected: BSONDocument = ["name": .string("")]
        #expect(delete.document == expected)
    }
    
    @Test func testDeleteBuilderFilterWithZero() {
        let delete = DeleteBuilder().filter("count", equals: 0)
        let expected: BSONDocument = ["count": .int64(0)]
        #expect(delete.document == expected)
    }
    
    @Test func testDeleteBuilderFilterWithMaxInt() {
        let delete = DeleteBuilder().filter("value", equals: Int.max)
        let expected: BSONDocument = ["value": .int64(Int64(Int.max))]
        #expect(delete.document == expected)
    }
    
    @Test func testDeleteBuilderFilterWithMinInt() {
        let delete = DeleteBuilder().filter("value", equals: Int.min)
        let expected: BSONDocument = ["value": .int64(Int64(Int.min))]
        #expect(delete.document == expected)
    }
    
    @Test func testDeleteBuilderFilterWithSpecialBSONValues() {
        let delete = DeleteBuilder().filter("field", equals: .null)
        let expected: BSONDocument = ["field": .null]
        #expect(delete.document == expected)
    }
}