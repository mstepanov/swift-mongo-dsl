import Testing
@testable import SwiftMongoDSL
import MongoSwift
import SwiftBSON
import Foundation

struct AggregationBuilderTests {
    
    // MARK: - Basic Pipeline Stages Tests
    
    @Test func testAggregationBuilderMatchWithQueryBuilderCreatesCorrectPipeline() {
        let query = QueryBuilder().where("status", equals: "active")
        let aggregation = AggregationBuilder().match(query)
        let expected: [BSONDocument] = [["$match": .document(["status": .string("active")])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderMatchWithBSONDocumentCreatesCorrectPipeline() {
        let filter: BSONDocument = ["status": .string("active")]
        let aggregation = AggregationBuilder().match(filter)
        let expected: [BSONDocument] = [["$match": .document(["status": .string("active")])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderGroupCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().group(.string("$category"), fields: [("count", .document(["$sum": .int32(1)]))])
        let expected: [BSONDocument] = [["$group": .document(["_id": .string("$category"), "count": .document(["$sum": .int32(1)])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderProjectCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().project([("name", .int32(1)), ("email", .int32(1))])
        let expected: [BSONDocument] = [["$project": .document(["name": .int32(1), "email": .int32(1)])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSortSingleFieldCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().sort("name", direction: .ascending)
        let expected: [BSONDocument] = [["$sort": .document(["name": .int32(1)])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSortMultipleFieldsCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().sort([("status", .ascending), ("name", .descending)])
        let expected: [BSONDocument] = [["$sort": .document(["status": .int32(1), "name": .int32(-1)])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderLimitCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().limit(10)
        let expected: [BSONDocument] = [["$limit": .int32(10)]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSkipCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().skip(5)
        let expected: [BSONDocument] = [["$skip": .int32(5)]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderLookupCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().lookup(from: "orders", localField: "userId", foreignField: "customerId", as: "userOrders")
        let expected: [BSONDocument] = [[
            "$lookup": .document([
                "from": .string("orders"),
                "localField": .string("userId"),
                "foreignField": .string("customerId"),
                "as": .string("userOrders")
            ])
        ]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderUnwindCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().unwind("tags")
        let expected: [BSONDocument] = [["$unwind": .string("$tags")]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderAddFieldsCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().addFields([("fullName", .document(["$concat": [.string("$firstName"), .string(" "), .string("$lastName")]]))])
        let expected: [BSONDocument] = [["$addFields": .document(["fullName": .document(["$concat": [.string("$firstName"), .string(" "), .string("$lastName")]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderReplaceRootCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().replaceRoot(newRoot: "details")
        let expected: [BSONDocument] = [["$replaceRoot": .document(["newRoot": .string("$details")])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderReplaceWithCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().replaceWith(newRoot: "details")
        let expected: [BSONDocument] = [["$replaceWith": .string("$details")]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSampleCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().sample(count: 5)
        let expected: [BSONDocument] = [["$sample": .document(["size": .int32(5)])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderIndexStatsCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().indexStats()
        let expected: [BSONDocument] = [["$indexStats": [:]]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderGeoNearCreatesCorrectPipeline() {
        let coordinates = [-73.99215, 40.73675]
        let aggregation = AggregationBuilder().geoNear(near: coordinates, distanceField: "dist.calculated")
        let expected: [BSONDocument] = [[
            "$geoNear": .document([
                "near": .array([.double(-73.99215), .double(40.73675)]),
                "distanceField": .string("dist.calculated"),
                "spherical": .bool(true)
            ])
        ]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderGeoNearWithSphericalFalseCreatesCorrectPipeline() {
        let coordinates = [-73.99215, 40.73675]
        let aggregation = AggregationBuilder().geoNear(near: coordinates, distanceField: "dist.calculated", spherical: false)
        let expected: [BSONDocument] = [[
            "$geoNear": .document([
                "near": .array([.double(-73.99215), .double(40.73675)]),
                "distanceField": .string("dist.calculated"),
                "spherical": .bool(false)
            ])
        ]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderFacetCreatesCorrectPipeline() {
        let pipeline1 = AggregationBuilder().match(QueryBuilder().where("status", equals: "A"))
        let pipeline2 = AggregationBuilder().group(.string("$cust_id"), fields: [("count", .document(["$sum": .int32(1)]))])
        
        let pipelines: [(String, [BSONDocument])] = [
            ("pipelineA", pipeline1.pipelineArray),
            ("pipelineB", pipeline2.pipelineArray)
        ]
        
        let aggregation = AggregationBuilder().facet(pipelines)
        let expected: [BSONDocument] = [[
            "$facet": .document([
                "pipelineA": .array([.document(["$match": .document(["status": .string("A")])])]),
                "pipelineB": .array([.document(["$group": .document(["_id": .string("$cust_id"), "count": .document(["$sum": .int32(1)])])])])
            ])
        ]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderBucketCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().bucket(
            groupBy: .string("$price"),
            boundaries: [.int32(0), .int32(10), .int32(20), .int32(30)],
            defaultBucket: .string("Other"),
            output: [("count", .document(["$sum": .int32(1)]))]
        )
        
        let expected: [BSONDocument] = [[
            "$bucket": .document([
                "groupBy": .string("$price"),
                "boundaries": .array([.int32(0), .int32(10), .int32(20), .int32(30)]),
                "default": .string("Other"),
                "output": .document(["count": .document(["$sum": .int32(1)])])
            ])
        ]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderBucketWithoutOptionalParamsCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().bucket(
            groupBy: .string("$price"),
            boundaries: [.int32(0), .int32(10), .int32(20), .int32(30)]
        )
        
        let expected: [BSONDocument] = [[
            "$bucket": .document([
                "groupBy": .string("$price"),
                "boundaries": .array([.int32(0), .int32(10), .int32(20), .int32(30)])
            ])
        ]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderBucketAutoCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().bucketAuto(
            groupBy: .string("$price"),
            buckets: 4,
            output: [("averagePrice", .document(["$avg": .string("$price")]))],
            granularity: "R5"
        )
        
        let expected: [BSONDocument] = [[
            "$bucketAuto": .document([
                "groupBy": .string("$price"),
                "buckets": .int32(4),
                "output": .document(["averagePrice": .document(["$avg": .string("$price")])]),
                "granularity": .string("R5")
            ])
        ]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderBucketAutoWithoutOptionalParamsCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().bucketAuto(
            groupBy: .string("$price"),
            buckets: 4
        )
        
        let expected: [BSONDocument] = [[
            "$bucketAuto": .document([
                "groupBy": .string("$price"),
                "buckets": .int32(4)
            ])
        ]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderCountCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().count(fieldName: "total")
        let expected: [BSONDocument] = [["$count": .string("total")]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    // MARK: - Arithmetic Operations Tests
    
    @Test func testAggregationBuilderAddCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().add(["field1", "field2"])
        let expected: [BSONDocument] = [["$project": .document(["sum": .document(["$add": [.string("$field1"), .string("$field2")]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderMultiplyCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().multiply(["field1", "field2"])
        let expected: [BSONDocument] = [["$project": .document(["product": .document(["$multiply": [.string("$field1"), .string("$field2")]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSubtractCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().subtract("field1", "field2")
        let expected: [BSONDocument] = [["$project": .document(["difference": .document(["$subtract": [.string("$field1"), .string("$field2")]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderDivideCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().divide("field1", "field2")
        let expected: [BSONDocument] = [["$project": .document(["quotient": .document(["$divide": [.string("$field1"), .string("$field2")]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderModCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().mod("field1", "field2")
        let expected: [BSONDocument] = [["$project": .document(["remainder": .document(["$mod": [.string("$field1"), .string("$field2")]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderAbsCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().abs("field1")
        let expected: [BSONDocument] = [["$project": .document(["absolute": .document(["$abs": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    // MARK: - String Operations Tests
    
    @Test func testAggregationBuilderConcatCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().concat(["firstName", "lastName"])
        let expected: [BSONDocument] = [["$project": .document(["concatenatedString": .document(["$concat": [.string("$firstName"), .string("$lastName")]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSubstrCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().substr("name", start: 0, length: 5)
        let expected: [BSONDocument] = [["$project": .document(["substring": .document(["$substr": .array([.string("$name"), .int32(0), .int32(5)])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSubstrBytesCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().substrBytes("name", start: 0, length: 5)
        let expected: [BSONDocument] = [["$project": .document(["substringBytes": .document(["$substrBytes": .array([.string("$name"), .int32(0), .int32(5)])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSubstrCPCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().substrCP("name", start: 0, length: 5)
        let expected: [BSONDocument] = [["$project": .document(["substringCP": .document(["$substrCP": .array([.string("$name"), .int32(0), .int32(5)])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderStrcasecmpCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().strcasecmp("field1", "field2")
        let expected: [BSONDocument] = [["$project": .document(["comparison": .document(["$strcasecmp": .array([.string("$field1"), .string("$field2")])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderRegexMatchCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().regexMatch("field", regex: "^A", options: "i")
        let expected: [BSONDocument] = [["$project": .document(["matches": .document(["$regexMatch": .document([
            "input": .string("$field"),
            "regex": .string("^A"),
            "options": .string("i")
        ])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderRegexMatchWithoutOptionsCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().regexMatch("field", regex: "^A")
        let expected: [BSONDocument] = [["$project": .document(["matches": .document(["$regexMatch": .document([
            "input": .string("$field"),
            "regex": .string("^A")
        ])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationStringBuilderToLowerCaseCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().toLower("name")
        let expected: [BSONDocument] = [["$project": .document(["lowercase": .document(["$toLower": .string("$name")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationStringBuilderToUpperCaseCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().toUpper("name")
        let expected: [BSONDocument] = [["$project": .document(["uppercase": .document(["$toUpper": .string("$name")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderTrimCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().trim("name")
        let expected: [BSONDocument] = [["$project": .document(["trimmed": .document(["$trim": .string("$name")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    // MARK: - Date Operations Tests
    
    @Test func testAggregationBuilderYearCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().year("createdAt")
        let expected: [BSONDocument] = [["$project": .document(["year": .document(["$year": .string("$createdAt")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderMonthCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().month("createdAt")
        let expected: [BSONDocument] = [["$project": .document(["month": .document(["$month": .string("$createdAt")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderDayOfMonthCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().dayOfMonth("createdAt")
        let expected: [BSONDocument] = [["$project": .document(["day": .document(["$dayOfMonth": .string("$createdAt")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderDateToStringCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().dateToString("createdAt", format: "%Y-%m-%d")
        let expected: [BSONDocument] = [["$project": .document(["formattedDate": .document(["$dateToString": .document([
            "date": .string("$createdAt"),
            "format": .string("%Y-%m-%d")
        ])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderDateToStringWithoutFormatCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().dateToString("createdAt")
        let expected: [BSONDocument] = [["$project": .document(["formattedDate": .document(["$dateToString": .document([
            "date": .string("$createdAt")
        ])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderDayOfYearCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().dayOfYear("createdAt")
        let expected: [BSONDocument] = [["$project": .document(["dayOfYear": .document(["$dayOfYear": .string("$createdAt")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderDayOfWeekCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().dayOfWeek("createdAt")
        let expected: [BSONDocument] = [["$project": .document(["dayOfWeek": .document(["$dayOfWeek": .string("$createdAt")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderHourCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().hour("createdAt")
        let expected: [BSONDocument] = [["$project": .document(["hour": .document(["$hour": .string("$createdAt")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderMinuteCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().minute("createdAt")
        let expected: [BSONDocument] = [["$project": .document(["minute": .document(["$minute": .string("$createdAt")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSecondCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().second("createdAt")
        let expected: [BSONDocument] = [["$project": .document(["second": .document(["$second": .string("$createdAt")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderMillisecondCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().millisecond("createdAt")
        let expected: [BSONDocument] = [["$project": .document(["millisecond": .document(["$millisecond": .string("$createdAt")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderWeekCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().week("createdAt")
        let expected: [BSONDocument] = [["$project": .document(["week": .document(["$week": .string("$createdAt")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderYearMonthDayCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().yearMonthDay("createdAt")
        let expected: [BSONDocument] = [["$project": .document([
            "year": .document(["$year": .string("$createdAt")]),
            "month": .document(["$month": .string("$createdAt")]),
            "day": .document(["$dayOfMonth": .string("$createdAt")])
        ])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderHourMinuteSecondCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().hourMinuteSecond("createdAt")
        let expected: [BSONDocument] = [["$project": .document([
            "hour": .document(["$hour": .string("$createdAt")]),
            "minute": .document(["$minute": .string("$createdAt")]),
            "second": .document(["$second": .string("$createdAt")])
        ])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    // MARK: - Conditional Operations Tests
    
    @Test func testAggregationBuilderCondCreatesCorrectPipeline() {
        let condition: BSONDocument = ["$gt": [.string("$qty"), .int32(250)]]
        let aggregation = AggregationBuilder().cond(if: condition, then: .string("A"), else: .string("B"))
        let expected: [BSONDocument] = [["$project": .document(["conditionalValue": .document(["$cond": .document(["if": .document(condition), "then": .string("A"), "else": .string("B")])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderIfNullCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().ifNull("description", then: .string("No description"))
        let expected: [BSONDocument] = [["$project": .document(["value": .document(["$ifNull": [.string("$description"), .string("No description")]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    // MARK: - Array Operations Tests
    
    @Test func testAggregationBuilderPushCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().push("items")
        let expected: [BSONDocument] = [["$group": .document(["_id": .string("null"), "items": .document(["$push": .string("$items")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSliceCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().slice("comments", count: 5)
        let expected: [BSONDocument] = [["$project": .document(["comments": .document(["$slice": [.string("$comments"), .int32(5)]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSliceWithSkipLimitCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().slice("comments", skip: 2, limit: 5)
        let expected: [BSONDocument] = [["$project": .document(["comments": .document(["$slice": [.string("$comments"), .int32(2), .int32(5)]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderAddToSetCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().addToSet("category")
        let expected: [BSONDocument] = [["$group": .document(["_id": .string("$category")])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderAvgCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().avg("price")
        let expected: [BSONDocument] = [["$group": .document(["_id": .string("null"), "avg": .document(["$avg": .string("$price")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSumCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().sum("quantity")
        let expected: [BSONDocument] = [["$group": .document(["_id": .string("null"), "total": .document(["$sum": .string("$quantity")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderFirstCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().first("date")
        let expected: [BSONDocument] = [["$group": .document(["_id": .string("null"), "first": .document(["$first": .string("$date")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderLastCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().last("date")
        let expected: [BSONDocument] = [["$group": .document(["_id": .string("null"), "last": .document(["$last": .string("$date")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderMinCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().min("price")
        let expected: [BSONDocument] = [["$group": .document(["_id": .string("null"), "min": .document(["$min": .string("$price")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderMaxCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().max("price")
        let expected: [BSONDocument] = [["$group": .document(["_id": .string("null"), "max": .document(["$max": .string("$price")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderPushWithFieldCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().push("items")
        let expected: [BSONDocument] = [["$group": .document(["_id": .string("null"), "items": .document(["$push": .string("$items")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderFilterCreatesCorrectPipeline() {
        let condition: BSONDocument = ["$gte": [.string("$$this.quantity"), .int32(10)]]
        let aggregation = AggregationBuilder().filter(array: "items", as: "item", cond: condition)
        let expected: [BSONDocument] = [["$project": .document(["items": .document(["$filter": .document([
            "input": .string("$items"),
            "as": .string("item"),
            "cond": .document(condition)
        ])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderMapCreatesCorrectPipeline() {
        let expression: BSON = .document(["$concat": [.string("$$this"), .string(" - processed")]])
        let aggregation = AggregationBuilder().map(array: "items", as: "item", in: expression)
        let expected: [BSONDocument] = [["$project": .document(["items": .document(["$map": .document([
            "input": .string("$items"),
            "as": .string("item"),
            "in": .document(["$concat": [.string("$$this"), .string(" - processed")]])
        ])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderZipCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().zip(["list1", "list2"])
        let expected: [BSONDocument] = [["$project": .document(["zipped": .document(["$zip": .document([
            "inputs": .array([.string("$list1"), .string("$list2")]),
            "useLongestLength": .bool(false)
        ])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderZipWithDefaultsCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().zip(["list1", "list2"], useLongestLength: true, defaults: [.int32(0), .string("default")])
        let expected: [BSONDocument] = [["$project": .document(["zipped": .document(["$zip": .document([
            "inputs": .array([.string("$list1"), .string("$list2")]),
            "useLongestLength": .bool(true),
            "defaults": .array([.int32(0), .string("default")])
        ])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderRangeCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().range(name: "numbers", start: 0, end: 10)
        let expected: [BSONDocument] = [["$project": .document(["numbers": .document(["$range": .array([.int32(0), .int32(10)])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderRangeWithStepCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().range(name: "numbers", start: 0, end: 10, step: 2)
        let expected: [BSONDocument] = [["$project": .document(["numbers": .document(["$range": .array([.int32(0), .int32(10), .int32(2)])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderReduceCreatesCorrectPipeline() {
        let expression: BSON = .document(["$add": [.string("$$value"), .string("$$this")]])
        let aggregation = AggregationBuilder().reduce(array: "items", initialValue: .string(""), in: expression)
        let expected: [BSONDocument] = [["$project": .document(["items": .document(["$reduce": .document([
            "input": .string("$items"),
            "initialValue": .string(""),
            "in": .document(["$add": [.string("$$value"), .string("$$this")]])
        ])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderReverseArrayCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().reverseArray("items")
        let expected: [BSONDocument] = [["$project": .document(["items": .document(["$reverseArray": .string("$items")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSizeCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().size("tags")
        let expected: [BSONDocument] = [["$project": .document(["arraySize": .document(["$size": .string("$tags")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderIsArrayCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().isArray("items")
        let expected: [BSONDocument] = [["$project": .document(["isItems": .document(["$isArray": .string("$items")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderConcatArraysCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().concatArrays(["array1", "array2"])
        let expected: [BSONDocument] = [["$project": .document(["concatenatedArray": .document(["$concatArrays": .array([.string("$array1"), .string("$array2")])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderArrayElemAtCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().arrayElemAt("items", index: 0)
        let expected: [BSONDocument] = [["$project": .document(["elementAt0": .document(["$arrayElemAt": .array([.string("$items"), .int32(0)])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderInCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().in("item", array: "itemsList")
        let expected: [BSONDocument] = [["$project": .document(["isItemInArray": .document(["$in": .array([.string("$item"), .string("$itemsList")])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    // MARK: - Set Operations Tests
    
    @Test func testAggregationBuilderSetEqualsCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().setEquals(["array1", "array2"])
        let expected: [BSONDocument] = [["$project": .document(["setsEqual": .document(["$setEquals": .array([.string("$array1"), .string("$array2")])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSetIntersectionCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().setIntersection(["array1", "array2"])
        let expected: [BSONDocument] = [["$project": .document(["intersection": .document(["$setIntersection": .array([.string("$array1"), .string("$array2")])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSetUnionAggCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().setUnionAgg(["array1", "array2"])
        let expected: [BSONDocument] = [["$project": .document(["union": .document(["$setUnion": .array([.string("$array1"), .string("$array2")])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSetDifferenceCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().setDifference("array1", "array2")
        let expected: [BSONDocument] = [["$project": .document(["difference": .document(["$setDifference": .array([.string("$array1"), .string("$array2")])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSetIsSubsetCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().setIsSubset("array1", "array2")
        let expected: [BSONDocument] = [["$project": .document(["isSubset": .document(["$setIsSubset": .array([.string("$array1"), .string("$array2")])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderAnyElementTrueCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().anyElementTrue("array1")
        let expected: [BSONDocument] = [["$project": .document(["anyTrue": .document(["$anyElementTrue": .string("$array1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderAllElementsTrueCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().allElementsTrue("array1")
        let expected: [BSONDocument] = [["$project": .document(["allTrue": .document(["$allElementsTrue": .string("$array1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    // MARK: - Other Operations Tests
    
    @Test func testAggregationBuilderSampleRateCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().sampleRate(0.1)
        let expected: [BSONDocument] = [["$sampleRate": .double(0.1)]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSetUnionCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().setUnion(["array1", "array2"])
        let expected: [BSONDocument] = [["$unionWith": .document(["coll": .array([.string("$array1"), .string("$array2")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    // MARK: - Additional Arithmetic Operations Tests
    
    @Test func testAggregationBuilderCeilCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().ceil("field1")
        let expected: [BSONDocument] = [["$project": .document(["ceiling": .document(["$ceil": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderFloorCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().floor("field1")
        let expected: [BSONDocument] = [["$project": .document(["floor": .document(["$floor": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderExpCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().exp("field1")
        let expected: [BSONDocument] = [["$project": .document(["exponential": .document(["$exp": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderLnCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().ln("field1")
        let expected: [BSONDocument] = [["$project": .document(["log": .document(["$ln": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderLogCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().log("field1")
        let expected: [BSONDocument] = [["$project": .document(["log10": .document(["$log10": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderLog10CreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().log10("field1")
        let expected: [BSONDocument] = [["$project": .document(["log10": .document(["$log10": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderPowCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().pow("field1", exponent: 2.0)
        let expected: [BSONDocument] = [["$project": .document(["power": .document(["$pow": [.string("$field1"), .double(2.0)]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSqrtCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().sqrt("field1")
        let expected: [BSONDocument] = [["$project": .document(["squareRoot": .document(["$sqrt": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSinCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().sin("field1")
        let expected: [BSONDocument] = [["$project": .document(["sine": .document(["$sin": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderCosCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().cos("field1")
        let expected: [BSONDocument] = [["$project": .document(["cosine": .document(["$cos": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderTanCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().tan("field1")
        let expected: [BSONDocument] = [["$project": .document(["tangent": .document(["$tan": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderAsinCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().asin("field1")
        let expected: [BSONDocument] = [["$project": .document(["arcsine": .document(["$asin": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderAcosCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().acos("field1")
        let expected: [BSONDocument] = [["$project": .document(["arccosine": .document(["$acos": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderAtanCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().atan("field1")
        let expected: [BSONDocument] = [["$project": .document(["arctangent": .document(["$atan": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderAtan2CreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().atan2("yField", "xField")
        let expected: [BSONDocument] = [["$project": .document(["arctangent2": .document(["$atan2": [.string("$yField"), .string("$xField")]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderAsinhCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().asinh("field1")
        let expected: [BSONDocument] = [["$project": .document(["hyperbolicAsin": .document(["$asinh": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderAcoshCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().acosh("field1")
        let expected: [BSONDocument] = [["$project": .document(["hyperbolicAcos": .document(["$acosh": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderAtanhCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().atanh("field1")
        let expected: [BSONDocument] = [["$project": .document(["hyperbolicAtan": .document(["$atanh": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSinhCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().sinh("field1")
        let expected: [BSONDocument] = [["$project": .document(["hyperbolicSin": .document(["$sinh": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderCoshCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().cosh("field1")
        let expected: [BSONDocument] = [["$project": .document(["hyperbolicCos": .document(["$cosh": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderTanhCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().tanh("field1")
        let expected: [BSONDocument] = [["$project": .document(["hyperbolicTan": .document(["$tanh": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderDegreesToRadiansCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().degreesToRadians("field1")
        let expected: [BSONDocument] = [["$project": .document(["radians": .document(["$degreesToRadians": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderRadiansToDegreesCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().radiansToDegrees("field1")
        let expected: [BSONDocument] = [["$project": .document(["degrees": .document(["$radiansToDegrees": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderRoundCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().round("field1", place: 2)
        let expected: [BSONDocument] = [["$project": .document(["rounded": .document(["$round": [.string("$field1"), .int32(2)]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderRoundWithoutPlaceCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().round("field1")
        let expected: [BSONDocument] = [["$project": .document(["rounded": .document(["$round": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderTruncCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().trunc("field1", place: 2)
        let expected: [BSONDocument] = [["$project": .document(["truncated": .document(["$trunc": [.string("$field1"), .int32(2)]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderTruncWithoutPlaceCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().trunc("field1")
        let expected: [BSONDocument] = [["$project": .document(["truncated": .document(["$trunc": .string("$field1")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderRandCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().rand()
        let expected: [BSONDocument] = [["$project": .document(["random": .document(["$rand": [:]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    // MARK: - Additional String Operations Tests
    
    @Test func testAggregationBuilderLtrimCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().ltrim("name")
        let expected: [BSONDocument] = [["$project": .document(["ltrimmed": .document(["$ltrim": .string("$name")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderRtrimCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().rtrim("name")
        let expected: [BSONDocument] = [["$project": .document(["rtrimmed": .document(["$rtrim": .string("$name")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderSplitCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().split("field", by: ",")
        let expected: [BSONDocument] = [["$project": .document(["splitArray": .document(["$split": [.string("$field"), .string(",")]])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderStrLenBytesCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().strLenBytes("field")
        let expected: [BSONDocument] = [["$project": .document(["byteLength": .document(["$strLenBytes": .string("$field")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    @Test func testAggregationBuilderStrLenCPCreatesCorrectPipeline() {
        let aggregation = AggregationBuilder().strLenCP("field")
        let expected: [BSONDocument] = [["$project": .document(["codePointLength": .document(["$strLenCP": .string("$field")])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    // MARK: - Conditional Operations Tests
    
    @Test func testAggregationBuilderSwitchCreatesCorrectPipeline() {
        let branches: [(case: BSONDocument, then: BSON)] = [
            (["$eq": [.string("$qty"), .int32(250)]], .string("A")),
            (["$gt": [.string("$qty"), .int32(250)]], .string("B"))
        ]
        let aggregation = AggregationBuilder().switch(branches: branches, default: .string("D"))
        let expected: [BSONDocument] = [["$project": .document(["switchedValue": .document(["$switch": .document([
            "branches": .array([
                .document(["case": .document(["$eq": [.string("$qty"), .int32(250)]]), "then": .string("A")]),
                .document(["case": .document(["$gt": [.string("$qty"), .int32(250)]]), "then": .string("B")])
            ]),
            "default": .string("D")
        ])])])]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    // MARK: - Other Operations Tests
    
    @Test func testAggregationBuilderRedactCreatesCorrectPipeline() {
        let expression: BSONDocument = ["$eq": [.string("$level"), .int32(1)]]
        let aggregation = AggregationBuilder().redact(expression: expression)
        let expected: [BSONDocument] = [["$redact": .document(expression)]]
        #expect(aggregation.pipelineArray == expected)
    }
    
    // MARK: - Method Chaining Tests
    
    @Test func testAggregationBuilderMethodChaining() {
        let aggregation = AggregationBuilder()
            .match(QueryBuilder().where("status", equals: "active"))
            .sort("name", direction: .ascending)
            .limit(10)
        
        let expected: [BSONDocument] = [
            ["$match": .document(["status": .string("active")])],
            ["$sort": .document(["name": .int32(1)])],
            ["$limit": .int32(10)]
        ]
        
        #expect(aggregation.pipelineArray.count == 3)
        #expect(aggregation.pipelineArray == expected)
    }
    
    // MARK: - Edge Cases Tests
    
    @Test func testAggregationBuilderEmptyPipeline() {
        let aggregation = AggregationBuilder()
        #expect(aggregation.pipelineArray.isEmpty)
    }
    
    @Test func testAggregationBuilderWithComplexPipeline() {
        let aggregation = AggregationBuilder()
            .match(QueryBuilder().where("status", equals: "active"))
            .group(.string("$category"), fields: [("count", .document(["$sum": .int32(1)]))])
            .sort("count", direction: .descending)
            .limit(5)
        
        #expect(aggregation.pipelineArray.count == 4)
        #expect(aggregation.pipelineArray[0].keys.contains("$match"))
        #expect(aggregation.pipelineArray[1].keys.contains("$group"))
        #expect(aggregation.pipelineArray[2].keys.contains("$sort"))
        #expect(aggregation.pipelineArray[3].keys.contains("$limit"))
    }
}