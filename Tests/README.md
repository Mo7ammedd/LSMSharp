# LSM-Tree Heavy Test Suite

This directory contains comprehensive tests for the LSM-Tree storage engine implementation.

## Test Categories

### 1. Functional Tests (`FunctionalTests.cs`)
- **Basic Operations**: Put, Get, Delete operations with various data types
- **Update Operations**: Multiple updates to the same key
- **Delete Operations**: Deletion validation and persistence
- **Range Queries**: Testing range-based data retrieval
- **Transaction Consistency**: Concurrent update consistency
- **Data Integrity**: Checksum-based data validation
- **Edge Cases**: Empty values, large keys, binary data, Unicode keys

### 2. Performance Tests (`PerformanceTests.cs`)
- **Sequential Write/Read**: Ordered operations performance
- **Random Write/Read**: Random access patterns
- **Concurrent Operations**: Multi-threaded performance
- **Mixed Workload**: Read/write ratio testing (70% reads, 30% writes)
- **Stress Testing**: High-volume operations

Test metrics include:
- Throughput (operations per second)
- Average latency
- Hit rates for read operations
- Memory usage tracking

### 3. Stress Tests (`StressTests.cs`)
- **Heavy Load**: 1M+ operations testing
- **Large Values**: 10KB value size testing
- **Concurrent Stress**: 100 concurrent threads
- **Memory Pressure**: Memory usage and cleanup validation
- **Compaction Stress**: Multi-level compaction testing
- **Crash Recovery**: Simulated crash and recovery validation

## Running Tests

### Run All Tests
```bash
cd Tests
dotnet run
```

### Run Specific Test Category
```bash
cd Tests
dotnet run functional    # Run only functional tests
dotnet run performance   # Run only performance tests
dotnet run stress        # Run only stress tests
```

### Build Tests
```bash
cd Tests
dotnet build
```

## Test Data

Tests create temporary databases in the following directories:
- `test_functional_db/` - Functional test data
- `test_performance_db/` - Performance test data  
- `test_stress_db/` - Stress test data
- `test_recovery_db/` - Recovery test data

These directories are automatically cleaned up before each test run.

## Expected Results

### Performance Benchmarks
- **Sequential Writes**: 50,000+ ops/sec
- **Sequential Reads**: 100,000+ ops/sec
- **Random Writes**: 30,000+ ops/sec
- **Random Reads**: 80,000+ ops/sec
- **Concurrent Operations**: 40,000+ ops/sec

### Stress Test Targets
- **Heavy Load**: 1M operations completion
- **Large Values**: 1000 × 10KB values
- **Concurrent Stress**: 100 threads × 10K ops each
- **Memory Efficiency**: < 500MB peak usage
- **Recovery Rate**: 99%+ data recovery after crash

## Notes

- Tests use fixed random seeds for reproducibility
- Performance results may vary based on hardware
- Stress tests may take 10+ minutes to complete
- All tests include data validation and integrity checks
- Tests automatically trigger compaction and flush operations
