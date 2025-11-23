import Testing
@testable import SwiftMongoDSL
import MongoSwift
import SwiftBSON
import Foundation

struct QueryBuilderTests {
    
    // MARK: - Basic Field Operations Tests
    
    @Test func testQueryBuilderWhereEqualsBSONCreatesCorrectFilter() {
        let query = QueryBuilder().where("name", equals: .string("John"))
        let expected: BSONDocument = ["name": .string("John")]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereEqualsStringCreatesCorrectFilter() {
        let query = QueryBuilder().where("name", equals: "John")
        let expected: BSONDocument = ["name": .string("John")]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereEqualsIntCreatesCorrectFilter() {
        let query = QueryBuilder().where("age", equals: 25)
        let expected: BSONDocument = ["age": .int64(25)]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereEqualsBoolCreatesCorrectFilter() {
        let query = QueryBuilder().where("active", equals: true)
        let expected: BSONDocument = ["active": .bool(true)]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereEqualsDoubleCreatesCorrectFilter() {
        let query = QueryBuilder().where("price", equals: 19.99)
        let expected: BSONDocument = ["price": .double(19.99)]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereEqualsDateCreatesCorrectFilter() {
        let date = Date(timeIntervalSince1970: 167888640) // March 15, 2023
        let query = QueryBuilder().where("createdAt", equals: date)
        let expected: BSONDocument = ["createdAt": .datetime(date)]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereEqualsExplicitCreatesCorrectFilter() {
        let query = QueryBuilder().where("name", equalsExplicit: .string("John"))
        let expected: BSONDocument = ["name": ["$eq": .string("John")]]
        #expect(query.document == expected)
    }
    
    // MARK: - Comparison Operators Tests
    
    @Test func testQueryBuilderWhereNotEqualsCreatesCorrectFilter() {
        let query = QueryBuilder().where("name", notEquals: .string("John"))
        let expected: BSONDocument = ["name": ["$ne": .string("John")]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereGreaterThanCreatesCorrectFilter() {
        let query = QueryBuilder().where("age", greaterThan: .int64(18))
        let expected: BSONDocument = ["age": ["$gt": .int64(18)]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereGreaterThanOrEqualCreatesCorrectFilter() {
        let query = QueryBuilder().where("age", greaterThanOrEqual: .int64(18))
        let expected: BSONDocument = ["age": ["$gte": .int64(18)]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereLessThanCreatesCorrectFilter() {
        let query = QueryBuilder().where("age", lessThan: .int64(65))
        let expected: BSONDocument = ["age": ["$lt": .int64(65)]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereLessThanOrEqualCreatesCorrectFilter() {
        let query = QueryBuilder().where("age", lessThanOrEqual: .int64(65))
        let expected: BSONDocument = ["age": ["$lte": .int64(65)]]
        #expect(query.document == expected)
    }
    
    // MARK: - Array Operators Tests
    
    @Test func testQueryBuilderWhereInCreatesCorrectFilter() {
        let query = QueryBuilder().where("status", in: [.string("active"), .string("pending")])
        let expected: BSONDocument = ["status": ["$in": .array([.string("active"), .string("pending")])]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereNotInCreatesCorrectFilter() {
        let query = QueryBuilder().where("status", notIn: [.string("inactive"), .string("deleted")])
        let expected: BSONDocument = ["status": ["$nin": .array([.string("inactive"), .string("deleted")])]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereExistsCreatesCorrectFilter() {
        let query = QueryBuilder().where("email", exists: true)
        let expected: BSONDocument = ["email": ["$exists": .bool(true)]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereTypeCreatesCorrectFilter() {
        let query = QueryBuilder().where("name", type: .string)
        let expected: BSONDocument = ["name": ["$type": .string("string")]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereMatchesRegexCreatesCorrectFilter() {
        let query = QueryBuilder().where("email", matches: "^[A-Za-z0-9._%+-]+@example.com$")
        let expected: BSONDocument = ["email": ["$regex": .string("^[A-Za-z0-9._%+-]+@example.com$")]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereMatchesRegexWithOptionsCreatesCorrectFilter() {
        let query = QueryBuilder().where("name", matches: "john", options: "i")
        let expected: BSONDocument = [
            "name": [
                "$regex": .string("john"),
                "$options": .string("i")
            ]
        ]
        #expect(query.document == expected)
    }
    
    // MARK: - Array Operations Tests
    
    @Test func testQueryBuilderWhereContainsCreatesCorrectFilter() {
        let query = QueryBuilder().where("tags", contains: .string("tech"))
        let expected: BSONDocument = ["tags": .string("tech")]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereContainsAllCreatesCorrectFilter() {
        let query = QueryBuilder().where("tags", containsAll: [.string("tech"), .string("swift")])
        let expected: BSONDocument = ["tags": ["$all": .array([.string("tech"), .string("swift")])]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereSizeCreatesCorrectFilter() {
        let query = QueryBuilder().where("tags", size: 3)
        let expected: BSONDocument = ["tags": ["$size": .int32(3)]]
        #expect(query.document == expected)
    }
    
    // MARK: - Nested Field Operations Tests
    
    @Test func testQueryBuilderWhereNestedEqualsCreatesCorrectFilter() {
        let query = QueryBuilder().where("address.city", nestedEquals: .string("New York"))
        let expected: BSONDocument = ["address.city": .string("New York")]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereNestedOperatorCreatesCorrectFilter() {
        let query = QueryBuilder().where("address.zip", nestedOperator: "$regex", value: .string("^10"))
        let expected: BSONDocument = ["address.zip": ["$regex": .string("^10")]]
        #expect(query.document == expected)
    }
    
    // MARK: - Logical Operations Tests
    
    @Test func testQueryBuilderAndCreatesCorrectFilter() {
        let condition1: BSONDocument = ["status": .string("active")]
        let condition2: BSONDocument = ["age": ["$gte": .int64(18)]]
        let query = QueryBuilder().and([condition1, condition2])
        let expected: BSONDocument = ["$and": .array([.document(condition1), .document(condition2)])]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderOrCreatesCorrectFilter() {
        let condition1: BSONDocument = ["status": .string("active")]
        let condition2: BSONDocument = ["status": .string("pending")]
        let query = QueryBuilder().or([condition1, condition2])
        let expected: BSONDocument = ["$or": .array([.document(condition1), .document(condition2)])]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderNotCreatesCorrectFilter() {
        let condition: BSONDocument = ["status": .string("inactive")]
        let query = QueryBuilder().not(condition)
        let expected: BSONDocument = ["$not": .document(condition)]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderNorCreatesCorrectFilter() {
        let condition1: BSONDocument = ["status": .string("active")]
        let condition2: BSONDocument = ["status": .string("pending")]
        let query = QueryBuilder().nor([condition1, condition2])
        let expected: BSONDocument = ["$nor": .array([.document(condition1), .document(condition2)])]
        #expect(query.document == expected)
    }
    
    // MARK: - Special Operators Tests
    
    @Test func testQueryBuilderWhereModuloCreatesCorrectFilter() {
        let query = QueryBuilder().where("number", modulo: (divisor: 5, remainder: 0))
        let expected: BSONDocument = ["number": ["$mod": .array([.int64(5), .int64(0)])]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereExprCreatesCorrectFilter() {
        let expr: BSONDocument = ["$gt": [.string("$field1"), .int64(100)]]
        let query = QueryBuilder().where("field2", expr: expr)
        let expected: BSONDocument = ["$expr": .document(expr)]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderTextSearchCreatesCorrectFilter() {
        let query = QueryBuilder().textSearch("mongodb")
        let expected: BSONDocument = ["$text": ["$search": .string("mongodb"), "$caseSensitive": .bool(false), "$diacriticSensitive": .bool(false)]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderTextSearchWithLanguageCreatesCorrectFilter() {
        let query = QueryBuilder().textSearch("mongodb", language: "english")
        let expected: BSONDocument = [
            "$text": [
                "$search": .string("mongodb"),
                "$language": .string("english"),
                "$caseSensitive": .bool(false),
                "$diacriticSensitive": .bool(false)
            ]
        ]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderTextSearchWithOptionsCreatesCorrectFilter() {
        let query = QueryBuilder().textSearch("mongodb", caseSensitive: true, diacriticSensitive: true)
        let expected: BSONDocument = [
            "$text": [
                "$search": .string("mongodb"),
                "$caseSensitive": .bool(true),
                "$diacriticSensitive": .bool(true)
            ]
        ]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereNearCreatesCorrectFilter() {
        let coordinates = [-73.99215, 40.73675]
        let query = QueryBuilder().where("location", near: coordinates)
        let expected: BSONDocument = [
            "location": [
                "$near": [
                    "$geometry": [
                        "type": "Point",
                        "coordinates": .array([.double(-73.99215), .double(40.73675)])
                    ]
                ]
            ]
        ]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereNearWithMaxDistanceCreatesCorrectFilter() {
        let coordinates = [-73.99215, 40.73675]
        let query = QueryBuilder().where("location", near: coordinates, maxDistance: 1000)
        let expected: BSONDocument = [
            "location": [
                "$near": [
                    "$geometry": [
                        "type": "Point",
                        "coordinates": .array([.double(-73.99215), .double(40.73675)])
                    ],
                    "$maxDistance": .double(1000)
                ]
            ]
        ]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereNearSphereCreatesCorrectFilter() {
        let coordinates = [-73.99215, 40.73675]
        let query = QueryBuilder().where("location", nearSphere: coordinates)
        let expected: BSONDocument = [
            "location": [
                "$nearSphere": [
                    "$geometry": [
                        "type": "Point",
                        "coordinates": .array([.double(-73.99215), .double(40.73675)])
                    ]
                ]
            ]
        ]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereElemMatchCreatesCorrectFilter() {
        let condition: BSONDocument = ["score": ["$gte": .int64(80)]]
        let query = QueryBuilder().where("results", elemMatch: condition)
        let expected: BSONDocument = ["results": ["$elemMatch": .document(condition)]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereAllCreatesCorrectFilter() {
        let query = QueryBuilder().where("tags", all: [.string("tech"), .string("programming")])
        let expected: BSONDocument = ["tags": ["$all": .array([.string("tech"), .string("programming")])]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereTypeWithNumberCreatesCorrectFilter() {
        let query = QueryBuilder().where("value", type: 2)  // String type code
        let expected: BSONDocument = ["value": ["$type": .int32(2)]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereIsNullCreatesCorrectFilter() {
        let query = QueryBuilder().where("optionalField", isNull: true)
        let expected: BSONDocument = ["optionalField": .null]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereIsNotNullCreatesCorrectFilter() {
        let query = QueryBuilder().where("optionalField", isNull: false)
        let expected: BSONDocument = ["optionalField": ["$ne": .null]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereMissingCreatesCorrectFilter() {
        let query = QueryBuilder().where("field", missing: true)
        let expected: BSONDocument = ["field": ["$exists": .bool(false)]]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereExistsNotMissingCreatesCorrectFilter() {
        let query = QueryBuilder().where("field", missing: false)
        let expected: BSONDocument = ["field": ["$exists": .bool(true)]]
        #expect(query.document == expected)
    }
    
    // MARK: - Query Options Tests
    
    @Test func testQueryBuilderSortSingleFieldAscending() {
        let query = QueryBuilder().sort("name", direction: .ascending)
        let expectedFilter: BSONDocument = [:]
        let expectedSort: BSONDocument = ["name": .int32(1)]
        #expect(query.document == expectedFilter)
        #expect(query.options.sort == expectedSort)
    }
    
    @Test func testQueryBuilderSortSingleFieldDescending() {
        let query = QueryBuilder().sort("name", direction: .descending)
        let expectedFilter: BSONDocument = [:]
        let expectedSort: BSONDocument = ["name": .int32(-1)]
        #expect(query.document == expectedFilter)
        #expect(query.options.sort == expectedSort)
    }
    
    @Test func testQueryBuilderSortMultipleFields() {
        let query = QueryBuilder().sort([("status", .ascending), ("name", .descending)])
        let expectedFilter: BSONDocument = [:]
        let expectedSort: BSONDocument = ["status": .int32(1), "name": .int32(-1)]
        #expect(query.document == expectedFilter)
        #expect(query.options.sort == expectedSort)
    }
    
    @Test func testQueryBuilderLimit() {
        let query = QueryBuilder().limit(10)
        let expectedFilter: BSONDocument = [:]
        #expect(query.document == expectedFilter)
        #expect(query.options.limit == 10)
    }
    
    @Test func testQueryBuilderSkip() {
        let query = QueryBuilder().skip(5)
        let expectedFilter: BSONDocument = [:]
        #expect(query.document == expectedFilter)
        #expect(query.options.skip == 5)
    }
    
    @Test func testQueryBuilderSelect() {
        let query = QueryBuilder().select("name", "email")
        let expectedFilter: BSONDocument = [:]
        let expectedProjection: BSONDocument = ["name": .int32(1), "email": .int32(1)]
        #expect(query.document == expectedFilter)
        #expect(query.options.projection == expectedProjection)
    }
    
    @Test func testQueryBuilderSelectArray() {
        let query = QueryBuilder().select(["name", "email"])
        let expectedFilter: BSONDocument = [:]
        let expectedProjection: BSONDocument = ["name": .int32(1), "email": .int32(1)]
        #expect(query.document == expectedFilter)
        #expect(query.options.projection == expectedProjection)
    }
    
    @Test func testQueryBuilderExclude() {
        let query = QueryBuilder().exclude("password", "internal")
        let expectedFilter: BSONDocument = [:]
        let expectedProjection: BSONDocument = ["password": .int32(0), "internal": .int32(0)]
        #expect(query.document == expectedFilter)
        #expect(query.options.projection == expectedProjection)
    }
    
    @Test func testQueryBuilderExcludeArray() {
        let query = QueryBuilder().exclude(["password", "internal"])
        let expectedFilter: BSONDocument = [:]
        let expectedProjection: BSONDocument = ["password": .int32(0), "internal": .int32(0)]
        #expect(query.document == expectedFilter)
        #expect(query.options.projection == expectedProjection)
    }
    
    @Test func testQueryBuilderSliceProjection() {
        let query = QueryBuilder().slice("comments", count: 5)
        let expectedFilter: BSONDocument = [:]
        let expectedProjection: BSONDocument = ["comments": ["$slice": .int32(5)]]
        #expect(query.document == expectedFilter)
        #expect(query.options.projection == expectedProjection)
    }
    
    @Test func testQueryBuilderSliceProjectionWithSkipLimit() {
        let query = QueryBuilder().slice("comments", skip: 2, limit: 5)
        let expectedFilter: BSONDocument = [:]
        let expectedProjection: BSONDocument = ["comments": ["$slice": .array([.int32(2), .int32(5)])]]
        #expect(query.document == expectedFilter)
        #expect(query.options.projection == expectedProjection)
    }
    
    // MARK: - Method Chaining Tests
    
    @Test func testQueryBuilderMethodChaining() {
        let query = QueryBuilder()
            .where("status", equals: "active")
            .where("age", greaterThan: 18)
            .sort("name", direction: .ascending)
            .limit(10)
        
        let expectedFilter: BSONDocument = ["status": .string("active"), "age": ["$gt": .int64(18)]]
        let expectedSort: BSONDocument = ["name": .int32(1)]
        
        #expect(query.document == expectedFilter)
        #expect(query.options.sort == expectedSort)
        #expect(query.options.limit == 10)
    }
    
    @Test func testQueryBuilderComplexQuery() {
        let condition1: BSONDocument = ["category": .string("tech")]
        let condition2: BSONDocument = ["level": .string("advanced")]
        
        let query = QueryBuilder()
            .where("status", equals: "published")
            .or([condition1, condition2])
            .sort("createdAt", direction: .descending)
            .select("title", "author", "createdAt")
        
        let expectedFilter: BSONDocument = [
            "status": .string("published"),
            "$or": .array([.document(condition1), .document(condition2)])
        ]
        let expectedSort: BSONDocument = ["createdAt": .int32(-1)]
        let expectedProjection: BSONDocument = ["title": .int32(1), "author": .int32(1), "createdAt": .int32(1)]
        
        #expect(query.document == expectedFilter)
        #expect(query.options.sort == expectedSort)
        #expect(query.options.projection == expectedProjection)
    }
    
    // MARK: - Builder Immutability Tests
    
    @Test func testQueryBuilderImmutability() {
        let initialQuery = QueryBuilder().where("status", equals: "active")
        let query1 = initialQuery.where("age", greaterThan: 18)
        let query2 = initialQuery.where("type", equals: "premium")
        
        let expectedInitial: BSONDocument = ["status": .string("active")]
        let expectedQuery1: BSONDocument = ["status": .string("active"), "age": ["$gt": .int64(18)]]
        let expectedQuery2: BSONDocument = ["status": .string("active"), "type": .string("premium")]
        
        #expect(initialQuery.document == expectedInitial)
        #expect(query1.document == expectedQuery1)
        #expect(query2.document == expectedQuery2)
    }
    
    // MARK: - Edge Cases Tests
    
    @Test func testQueryBuilderWhereEqualsEmptyString() {
        let query = QueryBuilder().where("name", equals: "")
        let expected: BSONDocument = ["name": .string("")]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereEqualsZero() {
        let query = QueryBuilder().where("count", equals: 0)
        let expected: BSONDocument = ["count": .int64(0)]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereEqualsMaxInt() {
        let query = QueryBuilder().where("value", equals: Int.max)
        let expected: BSONDocument = ["value": .int64(Int64(Int.max))]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereEqualsMinInt() {
        let query = QueryBuilder().where("value", equals: Int.min)
        let expected: BSONDocument = ["value": .int64(Int64(Int.min))]
        #expect(query.document == expected)
    }
    
    @Test func testQueryBuilderWhereEqualsSpecialBSONValues() {
        let query = QueryBuilder().where("field", equals: .null)
        let expected: BSONDocument = ["field": .null]
        #expect(query.document == expected)
    }
}