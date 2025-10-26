# Contributing to LSMSharp

Thank you for your interest in contributing to LSMSharp! This document provides guidelines and instructions for contributing to this project.

## Getting Started

### Prerequisites

- .NET 8.0 SDK or later
- Git
- A code editor (Visual Studio, VS Code, or JetBrider)

### Building the Project

```bash
# Clone the repository
git clone https://github.com/Mo7ammedd/LSMSharp.git
cd LSMSharp

# Build the project
dotnet build --configuration Release

# Run tests
dotnet test Tests/Tests.csproj --configuration Release
```

## Development Workflow

1. **Fork the repository** on GitHub
2. **Clone your fork** locally
3. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
4. **Make your changes** following the coding standards below
5. **Test your changes** thoroughly
6. **Commit your changes** with clear commit messages
7. **Push to your fork** and submit a pull request

## Coding Standards

### C# Style Guide

- Follow standard C# naming conventions
- Use PascalCase for public members, camelCase for private fields
- Add XML documentation comments for all public APIs
- Keep methods focused and concise (prefer < 50 lines)
- Use meaningful variable and method names

### Code Example

```csharp
/// <summary>
/// Retrieves an entry from the database.
/// </summary>
/// <param name="key">The key to retrieve.</param>
/// <returns>The entry if found, null otherwise.</returns>
public async Task<Entry?> GetEntryAsync(string key)
{
    if (string.IsNullOrEmpty(key))
        throw new ArgumentException("Key cannot be null or empty", nameof(key));
    
    // Implementation...
}
```

## Testing

### Running Tests

```bash
# Run all tests
dotnet test Tests/Tests.csproj

# Run specific test categories
dotnet run --project Tests/Tests.csproj functional
dotnet run --project Tests/Tests.csproj performance
dotnet run --project Tests/Tests.csproj stress
```

### Writing Tests

- Add tests for all new features
- Ensure existing tests pass
- Include both positive and negative test cases
- Test edge cases and error conditions

## Pull Request Process

1. **Update documentation** if you're changing public APIs
2. **Add tests** for new functionality
3. **Update README.md** if adding significant features
4. **Ensure all tests pass** before submitting
5. **Keep PRs focused** - one feature or fix per PR
6. **Write clear PR descriptions** explaining what and why

### PR Title Format

- `feat: Add range scan functionality`
- `fix: Correct bloom filter serialization bug`
- `docs: Update API documentation`
- `test: Add compaction stress tests`
- `perf: Optimize memtable flush performance`

## Code Review

All submissions require review before merging. Reviewers will check:

- Code quality and style
- Test coverage
- Documentation completeness
- Performance implications
- Backward compatibility

## Areas for Contribution

### High Priority

- Performance optimizations
- Additional compression algorithms (Snappy, Zstandard)
- Enhanced monitoring and metrics
- Improved error handling and recovery

### Medium Priority

- Iterator improvements
- Snapshot isolation
- Transaction support
- Backup and restore utilities

### Documentation

- Additional usage examples
- Performance tuning guide
- Architecture deep-dive
- Video tutorials

## Questions?

Feel free to open an issue for:

- Bug reports
- Feature requests
- Documentation improvements
- General questions

Please use the issue templates when available.

## License

By contributing to LSMSharp, you agree that your contributions will be licensed under the MIT License.

Thank you for contributing to LSMSharp!
