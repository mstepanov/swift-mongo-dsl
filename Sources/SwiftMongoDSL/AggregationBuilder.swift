import Foundation
import MongoSwift
import SwiftBSON

// MARK: - Aggregation DSL Builder

public struct AggregationBuilder {
    private var pipeline: [BSONDocument] = []
    
    public init() {}
    
    /// Match stage
    @discardableResult
    public func match(_ query: QueryBuilder) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$match": .document(query.document)])
        return newBuilder
    }
    
    /// Match stage with raw BSON document
    @discardableResult
    public func match(_ filter: BSONDocument) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$match": .document(filter)])
        return newBuilder
    }
    
    /// Group stage
    @discardableResult
    public func group(_ id: BSON, fields: [(String, BSON)] = []) -> AggregationBuilder {
        var newBuilder = self
        var groupDoc: BSONDocument = ["_id": id]
        for (key, value) in fields {
            groupDoc[key] = value
        }
        newBuilder.pipeline.append(["$group": .document(groupDoc)])
        return newBuilder
    }
    
    
    /// Project stage
    @discardableResult
    public func project(_ fields: [(String, BSON)]) -> AggregationBuilder {
        var newBuilder = self
        var projectDoc = BSONDocument()
        for (key, value) in fields {
            projectDoc[key] = value
        }
        newBuilder.pipeline.append(["$project": .document(projectDoc)])
        return newBuilder
    }
    
    
    /// Sort stage
    @discardableResult
    public func sort(_ field: String, direction: SortOrder = .ascending) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$sort": .document([field: .int32(direction.rawValue)])])
        return newBuilder
    }
    
    /// Sort by multiple fields
    @discardableResult
    public func sort(_ sortFields: [(String, SortOrder)]) -> AggregationBuilder {
        var newBuilder = self
        var sortDoc = BSONDocument()
        for (field, order) in sortFields {
            sortDoc[field] = .int32(order.rawValue)
        }
        newBuilder.pipeline.append(["$sort": .document(sortDoc)])
        return newBuilder
    }
    
    /// Limit stage
    @discardableResult
    public func limit(_ count: Int) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$limit": .int32(Int32(count))])
        return newBuilder
    }
    
    /// Skip stage
    @discardableResult
    public func skip(_ count: Int) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$skip": .int32(Int32(count))])
        return newBuilder
    }
    
    /// Lookup stage (join)
    @discardableResult
    public func lookup(from collection: String, localField: String, foreignField: String, as alias: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append([
            "$lookup": .document([
                "from": .string(collection),
                "localField": .string(localField),
                "foreignField": .string(foreignField),
                "as": .string(alias)
            ])
        ])
        return newBuilder
    }
    /// Unwind stage (deconstruct array)
    @discardableResult
    public func unwind(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$unwind": .string("$\(field)")])
        return newBuilder
    }
    
    /// AddFields stage (add new fields or modify existing ones)
    @discardableResult
    public func addFields(_ fields: [(String, BSON)]) -> AggregationBuilder {
        var newBuilder = self
        var addFieldsDoc = BSONDocument()
        for (key, value) in fields {
            addFieldsDoc[key] = value
        }
        newBuilder.pipeline.append(["$addFields": .document(addFieldsDoc)])
        return newBuilder
    }
    
    
    /// ReplaceRoot stage (replace document with specified embedded document)
    @discardableResult
    public func replaceRoot(newRoot: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$replaceRoot": .document(["newRoot": .string("$\(newRoot)")])])
        return newBuilder
    }
    
    /// ReplaceWith stage (replace document with specified embedded document)
    @discardableResult
    public func replaceWith(newRoot: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$replaceWith": .string("$\(newRoot)")])
        return newBuilder
    }
    
    /// Redact stage (restrict content based on information privileges)
    @discardableResult
    public func redact(expression: BSONDocument) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$redact": .document(expression)])
        return newBuilder
    }
    
    /// Sample stage (randomly select documents)
    @discardableResult
    public func sample(count: Int) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$sample": .document(["size": .int32(Int32(count))])])
        return newBuilder
    }
    
    /// IndexStats stage (return statistics for each index in the collection)
    @discardableResult
    public func indexStats() -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$indexStats": [:]])
        return newBuilder
    }
    
    /// GeoNear stage (return documents based on proximity to a geospatial point)
    @discardableResult
    public func geoNear(near: [Double], distanceField: String, spherical: Bool = true) -> AggregationBuilder {
        var newBuilder = self
        let geoNearDoc: BSONDocument = [
            "near": .array(near.map { .double($0) }),
            "distanceField": .string(distanceField),
            "spherical": .bool(spherical)
        ]
        newBuilder.pipeline.append(["$geoNear": .document(geoNearDoc)])
        return newBuilder
    }
    
    /// Facet stage (process multiple aggregation pipelines)
    @discardableResult
    public func facet(_ pipelines: [(String, [BSONDocument])]) -> AggregationBuilder {
        var newBuilder = self
        var facetDoc: BSONDocument = [:]
        // Iterate in the order provided by the array of tuples
        for (name, pipeline) in pipelines {
            facetDoc[name] = .array(pipeline.map { .document($0) })
        }
        newBuilder.pipeline.append(["$facet": .document(facetDoc)])
        return newBuilder
    }
    
    /// Bucket stage (categorize documents into groups)
    @discardableResult
    public func bucket(groupBy: BSON, boundaries: [BSON], defaultBucket: BSON? = nil, output: [(String, BSON)]? = nil) -> AggregationBuilder {
        var newBuilder = self
        var bucketDoc: BSONDocument = [
            "groupBy": groupBy,
            "boundaries": .array(boundaries)
        ]
        if let defaultVal = defaultBucket {
            bucketDoc["default"] = defaultVal
        }
        if let outputFields = output {
            var outputDoc: BSONDocument = [:]
            for (key, value) in outputFields {
                outputDoc[key] = value
            }
            bucketDoc["output"] = .document(outputDoc)
        }
        newBuilder.pipeline.append(["$bucket": .document(bucketDoc)])
        return newBuilder
    }
    
    
    /// BucketAuto stage (automatically categorize documents into groups)
    @discardableResult
    public func bucketAuto(groupBy: BSON, buckets: Int, output: [(String, BSON)]? = nil, granularity: String? = nil) -> AggregationBuilder {
        var newBuilder = self
        var bucketAutoDoc: BSONDocument = [
            "groupBy": groupBy,
            "buckets": .int32(Int32(buckets))
        ]
        if let outputFields = output {
            var outputDoc: BSONDocument = [:]
            for (key, value) in outputFields {
                outputDoc[key] = value
            }
            bucketAutoDoc["output"] = .document(outputDoc)
        }
        if let granularityVal = granularity {
            bucketAutoDoc["granularity"] = .string(granularityVal)
        }
        newBuilder.pipeline.append(["$bucketAuto": .document(bucketAutoDoc)])
        return newBuilder
    }
    
    
    /// Count stage (count documents in pipeline)
    @discardableResult
    public func count(fieldName: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$count": .string(fieldName)])
        return newBuilder
    }
    
    /// SetUnion stage (perform set operation to return all unique values)
    @discardableResult
    public func setUnion(_ fields: [String]) -> AggregationBuilder {
        var newBuilder = self
        let fieldExpressions = fields.map { "$\($0)" }.map { BSON.string($0) }
        newBuilder.pipeline.append(["$unionWith": .document(["coll": .array(fieldExpressions)])])
        return newBuilder
    }
    
    /// AddToSet stage (return an array of unique values)
    @discardableResult
    public func addToSet(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$group": .document(["_id": .string("$\(field)")])])
        return newBuilder
    }
    
    /// Avg stage (calculate average)
    @discardableResult
    public func avg(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$group": .document(["_id": .string("null"), "avg": .document(["$avg": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Sum stage (calculate sum)
    @discardableResult
    public func sum(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$group": .document(["_id": .string("null"), "total": .document(["$sum": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// First stage (return first document in group)
    @discardableResult
    public func first(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$group": .document(["_id": .string("null"), "first": .document(["$first": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Last stage (return last document in group)
    @discardableResult
    public func last(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$group": .document(["_id": .string("null"), "last": .document(["$last": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Min stage (return minimum value in group)
    @discardableResult
    public func min(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$group": .document(["_id": .string("null"), "min": .document(["$min": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Max stage (return maximum value in group)
    @discardableResult
    public func max(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$group": .document(["_id": .string("null"), "max": .document(["$max": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Push stage (return an array of values)
    @discardableResult
    public func push(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$group": .document(["_id": .string("null"), "items": .document(["$push": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Filter stage (returns an array after filtering out documents)
    @discardableResult
    public func filter(array: String, as: String, cond: BSONDocument) -> AggregationBuilder {
        var newBuilder = self
        let filterDoc: BSONDocument = [
            "input": .string("$\(array)"),
            "as": .string(`as`),
            "cond": .document(cond)
        ]
        newBuilder.pipeline.append(["$project": .document([array: .document(["$filter": .document(filterDoc)])])])
        return newBuilder
    }
    
    /// Map stage (applies expression to each item in array)
    @discardableResult
    public func map(array: String, as: String, in expression: BSON) -> AggregationBuilder {
        var newBuilder = self
        let mapDoc: BSONDocument = [
            "input": .string("$\(array)"),
            "as": .string(`as`),
            "in": expression
        ]
        newBuilder.pipeline.append(["$project": .document([array: .document(["$map": .document(mapDoc)])])])
        return newBuilder
    }
    
    /// Zip stage (merge arrays)
    @discardableResult
    public func zip(_ arrays: [String], useLongestLength: Bool = false, defaults: [BSON]? = nil) -> AggregationBuilder {
        var newBuilder = self
        var zipDoc: BSONDocument = [
            "inputs": .array(arrays.map { BSON.string("$\($0)") })
        ]
        zipDoc["useLongestLength"] = .bool(useLongestLength)
        if let defaultsArr = defaults {
            zipDoc["defaults"] = .array(defaultsArr)
        }
        newBuilder.pipeline.append(["$project": .document(["zipped": .document(["$zip": .document(zipDoc)])])])
        return newBuilder
    }
    
    /// Range stage (generates array of numbers)
    @discardableResult
    public func range(name: String, start: Int, end: Int, step: Int = 1) -> AggregationBuilder {
        var newBuilder = self
        var rangeArgs: [BSON] = [.int32(Int32(start)), .int32(Int32(end))]
        if step != 1 {
            rangeArgs.append(.int32(Int32(step)))
        }
        newBuilder.pipeline.append(["$project": .document([name: .document(["$range": .array(rangeArgs)])])])
        return newBuilder
    }
    
    /// Reduce stage (applies expression to each element in array)
    @discardableResult
    public func reduce(array: String, initialValue: BSON, in expression: BSON) -> AggregationBuilder {
        var newBuilder = self
        let reduceDoc: BSONDocument = [
            "input": .string("$\(array)"),
            "initialValue": initialValue,
            "in": expression
        ]
        newBuilder.pipeline.append(["$project": .document([array: .document(["$reduce": .document(reduceDoc)])])])
        return newBuilder
    }
    
    /// ReverseArray stage (returns reversed array)
    @discardableResult
    public func reverseArray(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document([field: .document(["$reverseArray": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Size stage (returns number of elements in array)
    @discardableResult
    public func size(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["arraySize": .document(["$size": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// IsArray stage (returns true if field is an array)
    @discardableResult
    public func isArray(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["is\(field.capitalized)": .document(["$isArray": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// ConcatArrays stage (concatenate arrays)
    @discardableResult
    public func concatArrays(_ arrays: [String]) -> AggregationBuilder {
        var newBuilder = self
        let arrayExprs = arrays.map { "$\($0)" }.map { BSON.string($0) }
        newBuilder.pipeline.append(["$project": .document(["concatenatedArray": .document(["$concatArrays": .array(arrayExprs)])])])
        return newBuilder
    }
    
    /// ArrayElemAt stage (returns element at specified index)
    @discardableResult
    public func arrayElemAt(_ field: String, index: Int) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["elementAt\(index)": .document(["$arrayElemAt": .array([.string("$\(field)"), .int32(Int32(index))])])])])
        return newBuilder
    }
    
    /// Slice stage (returns subset of array)
    @discardableResult
    public func slice(_ field: String, count: Int) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document([field: .document(["$slice": .array([.string("$\(field)"), .int32(Int32(count))])])])])
        return newBuilder
    }
    
    /// Slice stage with skip and limit
    @discardableResult
    public func slice(_ field: String, skip: Int, limit: Int) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document([field: .document(["$slice": .array([.string("$\(field)"), .int32(Int32(skip)), .int32(Int32(limit))])])])])
        return newBuilder
    }
    
    /// In stage (returns true if value is in array)
    @discardableResult
    public func `in`(_ field: String, array: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["is\(field.capitalized)InArray": .document(["$in": .array([.string("$\(field)"), .string("$\(array)")])])])])
        return newBuilder
    }
    
    /// SetEquals stage (compares arrays and returns true if they have same elements)
    @discardableResult
    public func setEquals(_ arrays: [String]) -> AggregationBuilder {
        var newBuilder = self
        let arrayExprs = arrays.map { "$\($0)" }.map { BSON.string($0) }
        newBuilder.pipeline.append(["$project": .document(["setsEqual": .document(["$setEquals": .array(arrayExprs)])])])
        return newBuilder
    }
    
    /// SetIntersection stage (returns common elements of arrays)
    @discardableResult
    public func setIntersection(_ arrays: [String]) -> AggregationBuilder {
        var newBuilder = self
        let arrayExprs = arrays.map { "$\($0)" }.map { BSON.string($0) }
        newBuilder.pipeline.append(["$project": .document(["intersection": .document(["$setIntersection": .array(arrayExprs)])])])
        return newBuilder
    }
    
    /// SetUnion stage (returns unique elements from arrays)
    @discardableResult
    public func setUnionAgg(_ arrays: [String]) -> AggregationBuilder {
        var newBuilder = self
        let arrayExprs = arrays.map { "$\($0)" }.map { BSON.string($0) }
        newBuilder.pipeline.append(["$project": .document(["union": .document(["$setUnion": .array(arrayExprs)])])])
        return newBuilder
    }
    
    /// SetDifference stage (returns elements in first array but not in second)
    @discardableResult
    public func setDifference(_ firstArray: String, _ secondArray: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["difference": .document(["$setDifference": .array([.string("$\(firstArray)"), .string("$\(secondArray)")])])])])
        return newBuilder
    }
    
    /// SetIsSubset stage (returns true if first array is subset of second)
    @discardableResult
    public func setIsSubset(_ firstArray: String, _ secondArray: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["isSubset": .document(["$setIsSubset": .array([.string("$\(firstArray)"), .string("$\(secondArray)")])])])])
        return newBuilder
    }
    
    /// AnyElementTrue stage (returns true if any element in array is true)
    @discardableResult
    public func anyElementTrue(_ array: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["anyTrue": .document(["$anyElementTrue": .string("$\(array)")])])])
        return newBuilder
    }
    
    /// AllElementsTrue stage (returns true if all elements in array are true)
    @discardableResult
    public func allElementsTrue(_ array: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["allTrue": .document(["$allElementsTrue": .string("$\(array)")])])])
        return newBuilder
    }
    
    /// Concat stage (concatenate strings)
    @discardableResult
    public func concat(_ strings: [String]) -> AggregationBuilder {
        var newBuilder = self
        let stringExprs = strings.map { "$\($0)" }.map { BSON.string($0) }
        newBuilder.pipeline.append(["$project": .document(["concatenatedString": .document(["$concat": .array(stringExprs)])])])
        return newBuilder
    }
    
    /// Substr stage (substring)
    @discardableResult
    public func substr(_ field: String, start: Int, length: Int) -> AggregationBuilder {
        var newBuilder = self
        let pipelineStage: BSONDocument = [
            "$project": [
                "substring": [
                    "$substr": [
                        .string("$\(field)"),
                        .int32(Int32(start)),
                        .int32(Int32(length))
                    ]
                ]
            ]
        ]
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// SubstrBytes stage (substring by bytes)
    @discardableResult
    public func substrBytes(_ field: String, start: Int, length: Int) -> AggregationBuilder {
        var newBuilder = self
        let pipelineStage: BSONDocument = [
            "$project": [
                "substringBytes": [
                    "$substrBytes": [
                        .string("$\(field)"),
                        .int32(Int32(start)),
                        .int32(Int32(length))
                    ]
                ]
            ]
        ]
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// SubstrCP stage (substring by code points)
    @discardableResult
    public func substrCP(_ field: String, start: Int, length: Int) -> AggregationBuilder {
        var newBuilder = self
        let pipelineStage: BSONDocument = [
            "$project": [
                "substringCP": [
                    "$substrCP": [
                        .string("$\(field)"),
                        .int32(Int32(start)),
                        .int32(Int32(length))
                    ]
                ]
            ]
        ]
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// ToLower stage (convert to lowercase)
    @discardableResult
    public func toLower(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["lowercase": .document(["$toLower": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// ToUpper stage (convert to uppercase)
    @discardableResult
    public func toUpper(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["uppercase": .document(["$toUpper": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Trim stage (remove whitespace)
    @discardableResult
    public func trim(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["trimmed": .document(["$trim": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// LTrim stage (remove leading whitespace)
    @discardableResult
    public func ltrim(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["ltrimmed": .document(["$ltrim": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// RTrim stage (remove trailing whitespace)
    @discardableResult
    public func rtrim(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["rtrimmed": .document(["$rtrim": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Split stage (split string by delimiter)
    @discardableResult
    public func split(_ field: String, by delimiter: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["splitArray": .document(["$split": .array([.string("$\(field)"), .string(delimiter)])])])])
        return newBuilder
    }
    
    /// StrLenBytes stage (string length in bytes)
    @discardableResult
    public func strLenBytes(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["byteLength": .document(["$strLenBytes": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// StrLenCP stage (string length in code points)
    @discardableResult
    public func strLenCP(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["codePointLength": .document(["$strLenCP": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Strcasecmp stage (case-insensitive string comparison)
    @discardableResult
    public func strcasecmp(_ firstField: String, _ secondField: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["comparison": .document(["$strcasecmp": .array([.string("$\(firstField)"), .string("$\(secondField)")])])])])
        return newBuilder
    }
    
    /// RegexMatch stage (regex pattern matching)
    @discardableResult
    public func regexMatch(_ field: String, regex: String, options: String? = nil) -> AggregationBuilder {
        var newBuilder = self
        var regexDoc: BSONDocument = [
            "input": .string("$\(field)"),
            "regex": .string(regex)
        ]
        if let opts = options {
            regexDoc["options"] = .string(opts)
        }
        newBuilder.pipeline.append(["$project": .document(["matches": .document(["$regexMatch": .document(regexDoc)])])])
        return newBuilder
    }
    
    /// DateToString stage (format date to string)
    @discardableResult
    public func dateToString(_ field: String, format: String? = nil) -> AggregationBuilder {
        var newBuilder = self
        var dateToStringDoc: BSONDocument = ["date": .string("$\(field)")]
        if let fmt = format {
            dateToStringDoc["format"] = .string(fmt)
        }
        newBuilder.pipeline.append(["$project": .document(["formattedDate": .document(["$dateToString": .document(dateToStringDoc)])])])
        return newBuilder
    }
    
    /// Year stage (extract year from date)
    @discardableResult
    public func year(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["year": .document(["$year": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Month stage (extract month from date)
    @discardableResult
    public func month(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["month": .document(["$month": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// DayOfMonth stage (extract day of month from date)
    @discardableResult
    public func dayOfMonth(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["day": .document(["$dayOfMonth": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// DayOfYear stage (extract day of year from date)
    @discardableResult
    public func dayOfYear(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["dayOfYear": .document(["$dayOfYear": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// DayOfWeek stage (extract day of week from date)
    @discardableResult
    public func dayOfWeek(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["dayOfWeek": .document(["$dayOfWeek": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Hour stage (extract hour from date)
    @discardableResult
    public func hour(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["hour": .document(["$hour": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Minute stage (extract minute from date)
    @discardableResult
    public func minute(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["minute": .document(["$minute": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Second stage (extract second from date)
    @discardableResult
    public func second(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["second": .document(["$second": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Millisecond stage (extract millisecond from date)
    @discardableResult
    public func millisecond(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["millisecond": .document(["$millisecond": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// Week stage (extract week from date)
    @discardableResult
    public func week(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["week": .document(["$week": .string("$\(field)")])])])
        return newBuilder
    }
    
    /// YearMonthDay stage (extract year, month, and day from date)
    @discardableResult
    public func yearMonthDay(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document([
            "year": .document(["$year": .string("$\(field)")]),
            "month": .document(["$month": .string("$\(field)")]),
            "day": .document(["$dayOfMonth": .string("$\(field)")])
        ])])
        return newBuilder
    }
    
    /// HourMinuteSecond stage (extract hour, minute, and second from date)
    @discardableResult
    public func hourMinuteSecond(_ field: String) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document([
            "hour": .document(["$hour": .string("$\(field)")]),
            "minute": .document(["$minute": .string("$\(field)")]),
            "second": .document(["$second": .string("$\(field)")])
        ])])
        return newBuilder
    }
    
    /// Cond stage (conditional expression)
    @discardableResult
    public func cond(if condition: BSONDocument, then trueValue: BSON, `else` falseValue: BSON) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["conditionalValue": .document(["$cond": .document([
            "if": .document(condition),
            "then": trueValue,
            "else": falseValue
        ])])])])
        return newBuilder
    }
    
    /// IfNull stage (return value if not null, otherwise return alternative)
    @discardableResult
    public func ifNull(_ field: String, then alternative: BSON) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$project": .document(["value": .document(["$ifNull": .array([.string("$\(field)"), alternative])])])])
        return newBuilder
    }
    
    /// Switch stage (multiple conditional expressions)
    @discardableResult
    public func `switch`(branches: [(case: BSONDocument, then: BSON)], default defaultResult: BSON) -> AggregationBuilder {
        var newBuilder = self
        var switchDoc: BSONDocument = [:]
        var branchArray: [BSONDocument] = []
        for (caseCondition, thenResult) in branches {
            branchArray.append([
                "case": .document(caseCondition),
                "then": thenResult
            ])
        }
        switchDoc["branches"] = .array(branchArray.map { .document($0) })
        switchDoc["default"] = defaultResult
        let pipelineStage: BSONDocument = [
            "$project": [
                "switchedValue": [
                    "$switch": .document(switchDoc)
                ]
            ]
        ]
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Add stage (arithmetic addition)
    @discardableResult
    public func add(_ fields: [String]) -> AggregationBuilder {
        var newBuilder = self
        let fieldExprs = fields.map { "$\($0)" }.map { BSON.string($0) }
        let pipelineStage: BSONDocument = [
            "$project": [
                "sum": [
                    "$add": .array(fieldExprs)
                ]
            ]
        ]
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Multiply stage (arithmetic multiplication)
    @discardableResult
    public func multiply(_ fields: [String]) -> AggregationBuilder {
        var newBuilder = self
        let fieldExprs = fields.map { "$\($0)" }.map { BSON.string($0) }
        let pipelineStage: BSONDocument = [
            "$project": [
                "product": [
                    "$multiply": .array(fieldExprs)
                ]
            ]
        ]
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Subtract stage (arithmetic subtraction)
    @discardableResult
    public func subtract(_ firstField: String, _ secondField: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "difference": [
                    "$subtract": [
                        .string("$\(firstField)"),
                        .string("$\(secondField)")
                    ]
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Divide stage (arithmetic division)
    @discardableResult
    public func divide(_ firstField: String, _ secondField: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "quotient": [
                    "$divide": [
                        .string("$\(firstField)"),
                        .string("$\(secondField)")
                    ]
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Mod stage (arithmetic modulo)
    @discardableResult
    public func mod(_ firstField: String, _ secondField: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "remainder": [
                    "$mod": [
                        .string("$\(firstField)"),
                        .string("$\(secondField)")
                    ]
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Abs stage (absolute value)
    @discardableResult
    public func abs(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "absolute": [
                    "$abs": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Ceil stage (ceiling value)
    @discardableResult
    public func ceil(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "ceiling": [
                    "$ceil": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Floor stage (floor value)
    @discardableResult
    public func floor(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "floor": [
                    "$floor": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Exp stage (e raised to power)
    @discardableResult
    public func exp(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "exponential": [
                    "$exp": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Ln stage (natural logarithm)
    @discardableResult
    public func ln(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "log": [
                    "$ln": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Log stage (logarithm base 10)
    @discardableResult
    public func log(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "log10": [
                    "$log10": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Log10 stage (logarithm base 10)
    @discardableResult
    public func log10(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "log10": [
                    "$log10": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Pow stage (power)
    @discardableResult
    public func pow(_ field: String, exponent: Double) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "power": [
                    "$pow": [
                        .string("$\(field)"),
                        .double(exponent)
                    ]
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Sqrt stage (square root)
    @discardableResult
    public func sqrt(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "squareRoot": [
                    "$sqrt": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Sin stage (sine)
    @discardableResult
    public func sin(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "sine": [
                    "$sin": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Cos stage (cosine)
    @discardableResult
    public func cos(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "cosine": [
                    "$cos": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Tan stage (tangent)
    @discardableResult
    public func tan(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "tangent": [
                    "$tan": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Asin stage (arcsine)
    @discardableResult
    public func asin(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "arcsine": [
                    "$asin": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Acos stage (arccosine)
    @discardableResult
    public func acos(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "arccosine": [
                    "$acos": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Atan stage (arctangent)
    @discardableResult
    public func atan(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "arctangent": [
                    "$atan": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Atan2 stage (arctangent of y/x)
    @discardableResult
    public func atan2(_ yField: String, _ xField: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "arctangent2": [
                    "$atan2": [
                        .string("$\(yField)"),
                        .string("$\(xField)")
                    ]
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Asinh stage (hyperbolic arcsine)
    @discardableResult
    public func asinh(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "hyperbolicAsin": [
                    "$asinh": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Acosh stage (hyperbolic arccosine)
    @discardableResult
    public func acosh(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "hyperbolicAcos": [
                    "$acosh": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Atanh stage (hyperbolic arctangent)
    @discardableResult
    public func atanh(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "hyperbolicAtan": [
                    "$atanh": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Sinh stage (hyperbolic sine)
    @discardableResult
    public func sinh(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "hyperbolicSin": [
                    "$sinh": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Cosh stage (hyperbolic cosine)
    @discardableResult
    public func cosh(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "hyperbolicCos": [
                    "$cosh": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Tanh stage (hyperbolic tangent)
    @discardableResult
    public func tanh(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "hyperbolicTan": [
                    "$tanh": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// DegreesToRadians stage (convert degrees to radians)
    @discardableResult
    public func degreesToRadians(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "radians": [
                    "$degreesToRadians": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// RadiansToDegrees stage (convert radians to degrees)
    @discardableResult
    public func radiansToDegrees(_ field: String) -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "degrees": [
                    "$radiansToDegrees": .string("$\(field)")
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// Round stage (round number)
    @discardableResult
    public func round(_ field: String, place: Int = 0) -> AggregationBuilder {
        var newBuilder = self
        if place == 0 {
            let pipelineStage: BSONDocument = [
                "$project": [
                    "rounded": [
                        "$round": .string("$\(field)")
                    ]
                ]
            ]
            newBuilder.pipeline.append(pipelineStage)
        } else {
            let pipelineStage: BSONDocument = [
                "$project": [
                    "rounded": [
                        "$round": [
                            .string("$\(field)"),
                            .int32(Int32(place))
                        ]
                    ]
                ]
            ]
            newBuilder.pipeline.append(pipelineStage)
        }
        return newBuilder
    }
    
    /// Trunc stage (truncate number)
    @discardableResult
    public func trunc(_ field: String, place: Int = 0) -> AggregationBuilder {
        var newBuilder = self
        if place == 0 {
            let pipelineStage: BSONDocument = [
                "$project": [
                    "truncated": [
                        "$trunc": .string("$\(field)")
                    ]
                ]
            ]
            newBuilder.pipeline.append(pipelineStage)
        } else {
            let pipelineStage: BSONDocument = [
                "$project": [
                    "truncated": [
                        "$trunc": [
                            .string("$\(field)"),
                            .int32(Int32(place))
                        ]
                    ]
                ]
            ]
            newBuilder.pipeline.append(pipelineStage)
        }
        return newBuilder
    }
    
    /// Rand stage (generate random number)
    @discardableResult
    public func rand() -> AggregationBuilder {
        let pipelineStage: BSONDocument = [
            "$project": [
                "random": [
                    "$rand": [:]
                ]
            ]
        ]
        var newBuilder = self
        newBuilder.pipeline.append(pipelineStage)
        return newBuilder
    }
    
    /// SampleRate stage (sample documents by rate)
    @discardableResult
    public func sampleRate(_ rate: Double) -> AggregationBuilder {
        var newBuilder = self
        newBuilder.pipeline.append(["$sampleRate": .double(rate)])
        return newBuilder
    }
    
    /// Execute the aggregation pipeline
    public func execute(in collection: MongoCollection<BSONDocument>) async throws -> MongoCursor<BSONDocument> {
        return try await collection.aggregate(pipeline)
    }
    
    /// Get the underlying pipeline
    public var pipelineArray: [BSONDocument] {
        return pipeline
    }
}
