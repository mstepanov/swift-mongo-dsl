// swift-tools-version: 6.2

import PackageDescription

let package = Package(
    name: "swift-mongo-dsl",
    platforms: [
        .macOS(.v10_15),
    ],
    products: [
        .library(
            name: "SwiftMongoDSL",
            targets: ["SwiftMongoDSL"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/mongodb/swift-bson", .upToNextMajor(from: "3.0.0")),
        .package(url: "https://github.com/mongodb/mongo-swift-driver", .upToNextMajor(from: "1.3.1"))
    ],
    targets: [
        .target(
            name: "SwiftMongoDSL",
            dependencies: [
                .product(name: "MongoSwift", package: "mongo-swift-driver"),
                .product(name: "SwiftBSON", package: "swift-bson")
            ]
        ),
        .testTarget(
            name: "SwiftMongoDSLTests",
            dependencies: ["SwiftMongoDSL"]
        ),
    ]
)
