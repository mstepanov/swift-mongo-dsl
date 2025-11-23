import Testing
@testable import SwiftMongoDSL
import MongoSwift
import SwiftBSON
import Foundation

struct SwiftMongoDSLExtensionsTests {
    
    // MARK: - Collection Extensions Tests
    
    @Test func testMongoCollectionQueryExtensionCreatesCorrectDocument() {
        let queryDocument = MongoCollection<BSONDocument>.query { query in
            query.where("status", equals: "active")
        }
        let expected: BSONDocument = ["status": .string("active")]
        #expect(queryDocument == expected)
    }
    
    @Test func testMongoCollectionUpdateExtensionCreatesCorrectDocument() {
        let updateDocument = MongoCollection<BSONDocument>.update { update in
            update.set("status", to: "updated")
        }
        let expected: BSONDocument = ["$set": ["status": .string("updated")]]
        #expect(updateDocument == expected)
    }
    
    @Test func testMongoCollectionDeleteExtensionCreatesCorrectDocument() {
        let deleteDocument = MongoCollection<BSONDocument>.delete { delete in
            delete.filter("status", equals: "inactive")
        }
        let expected: BSONDocument = ["status": .string("inactive")]
        #expect(deleteDocument == expected)
    }
    
    @Test func testMongoCollectionAggregateExtensionCreatesCorrectPipeline() {
        let pipeline = MongoCollection<BSONDocument>.aggregate { agg in
            agg.match(QueryBuilder().where("status", equals: "active"))
                .limit(10)
        }
        let expected: [BSONDocument] = [
            ["$match": .document(["status": .string("active")])],
            ["$limit": .int32(10)]
        ]
        #expect(pipeline == expected)
    }
    
    // MARK: - BSONDocument Extensions Tests
    
    @Test func testBSONDocumentQueryExtensionCreatesCorrectDocument() {
        let queryDocument = BSONDocument.query { query in
            query.where("name", equals: "John")
        }
        let expected: BSONDocument = ["name": .string("John")]
        #expect(queryDocument == expected)
    }
    
    @Test func testBSONDocumentUpdateExtensionCreatesCorrectDocument() {
        let updateDocument = BSONDocument.update { update in
            update.set("name", to: "Jane")
        }
        let expected: BSONDocument = ["$set": ["name": .string("Jane")]]
        #expect(updateDocument == expected)
    }
    
    @Test func testBSONDocumentDeleteExtensionCreatesCorrectDocument() {
        let deleteDocument = BSONDocument.delete { delete in
            delete.filter("name", equals: "John")
        }
        let expected: BSONDocument = ["name": .string("John")]
        #expect(deleteDocument == expected)
    }
    
    @Test func testBSONDocumentAggregateExtensionCreatesCorrectPipeline() {
        let pipeline = BSONDocument.aggregate { agg in
            agg.match(QueryBuilder().where("status", equals: "active"))
                .sort("name", direction: .ascending)
        }
        let expected: [BSONDocument] = [
            ["$match": .document(["status": .string("active")])],
            ["$sort": .document(["name": .int32(1)])]
        ]
        #expect(pipeline == expected)
    }
    
    // MARK: - Complex Operations Tests
    
    @Test func testComplexQueryWithCollectionExtension() {
        let queryDocument = MongoCollection<BSONDocument>.query { query in
            query.where("status", equals: "active")
                 .where("age", greaterThan: 18)
                 .sort("name", direction: .ascending)
                 .limit(10)
        }
        let expectedFilter: BSONDocument = ["status": .string("active"), "age": ["$gt": .int64(18)]]
        #expect(queryDocument == expectedFilter)
    }
    
    @Test func testComplexUpdateWithCollectionExtension() {
        let updateDocument = MongoCollection<BSONDocument>.update { update in
            update.set("lastModified", to: Date())
                 .increment("updateCount", by: 1)
        }
        #expect(updateDocument.keys.contains("$set"))
        #expect(updateDocument.keys.contains("$inc"))
    }
    
    @Test func testComplexAggregateWithCollectionExtension() {
        let pipeline = MongoCollection<BSONDocument>.aggregate { agg in
            agg.match(QueryBuilder().where("status", equals: "active"))
                .group(.string("$category"), fields: [("count", .document(["$sum": .int32(1)]))])
                .sort("count", direction: .descending)
        }
        #expect(pipeline.count == 3)
        #expect(pipeline[0].keys.contains("$match"))
        #expect(pipeline[1].keys.contains("$group"))
        #expect(pipeline[2].keys.contains("$sort"))
    }
    
    // MARK: - Edge Cases Tests
    
    @Test func testEmptyQueryExtension() {
        let queryDocument = BSONDocument.query { query in
            query  // Return query as is without any conditions
        }
        let expected: BSONDocument = [:]
        #expect(queryDocument == expected)
    }
    
    @Test func testEmptyUpdateExtension() {
        let updateDocument = BSONDocument.update { update in
            update  // Return update as is without any operations
        }
        let expected: BSONDocument = [:]
        #expect(updateDocument == expected)
    }
    
    @Test func testEmptyAggregateExtension() {
        let pipeline = BSONDocument.aggregate { agg in
            agg  // Return aggregate as is without any operations
        }
        #expect(pipeline.isEmpty)
    }
}