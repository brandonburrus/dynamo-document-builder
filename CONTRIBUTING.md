# Contributing to Dynamo Document Builder

All contributions from the community are welcome, whether it's bug reports, feature requests, documentation improvements, or code contributions!

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Pull Request Process](#pull-request-process)
- [Coding Guidelines](#coding-guidelines)
- [Testing](#testing)
- [Documentation](#documentation)
- [Release Process](#release-process)

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for everyone. Be kind, be professional, and be collaborative.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/dynamo-document-builder.git
   cd dynamo-document-builder
   ```
3. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/brandonburrus/dynamo-document-builder.git
   ```

## Development Setup

### Prerequisites

- Node.js 20.x or higher
- npm 9.x or higher
- Git

### Installation

1. Install dependencies:
   ```bash
   npm clean-install
   ```

2. Build the project:
   ```bash
   npm run build
   ```

3. Run unit tests to ensure everything works:
   ```bash
   npm test
   ```

### Available Scripts

- `npm run build` - Build the project for production
- `npm run format` - Format code using Biome
- `npm run lint` - Lint and auto-fix issues
- `npm run typecheck` - Run TypeScript type checking
- `npm test` - Run unit tests
- `npm run test:coverage` - Run tests with coverage report
- `npm run test:watch` - Run tests in watch mode
- `npm run test:integration` - Run integration tests
- `npm run docs` - Run documentation site locally

## How to Contribute

### Reporting Bugs

If you find a bug, please open an issue on [GitHub Issues](https://github.com/brandonburrus/dynamo-document-builder/issues) with:

- A clear, descriptive title
- Steps to reproduce the issue
- Expected behavior vs actual behavior
- Your environment (Node.js version, OS, etc.)
- Code samples or test cases (if applicable)

### Suggesting Features

We welcome feature suggestions! Please open an issue with:

- A clear description of the feature
- Use cases and benefits
- Any potential implementation ideas
- Examples of how the API might look

### Contributing Code

1. **Find or create an issue** describing what you plan to work on
2. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes** following our [coding guidelines](#coding-guidelines)
4. **Write tests** for your changes
5. **Ensure all tests pass**:
   ```bash
   npm test
   npm run test:integration
   npm run typecheck
   npm run lint
   ```
6. **Commit your changes** using conventional commits:
   ```bash
   git commit -m "feat: add new feature"
   ```
7. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```
8. **Open a Pull Request** on GitHub

## Pull Request Process

1. Fill out the pull request template completely
2. Link any related issues using keywords (e.g., "Fixes #123")
3. Ensure all CI checks pass
4. Request a review from maintainers
5. Address any feedback promptly
6. Once approved, a maintainer will merge your PR

### PR Requirements

- [ ] All tests pass
- [ ] Code is properly formatted and linted
- [ ] Type checking passes
- [ ] New features include tests
- [ ] Documentation is updated (if applicable)
- [ ] Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)

## Coding Guidelines

### TypeScript Style

- Use TypeScript for all code
- Prefer `const` over `let`, avoid `var`
- Use explicit types where it improves clarity
- Prefer interfaces over type aliases for objects
- Use meaningful variable and function names

### Code Formatting

We use [Biome](https://biomejs.dev/) for formatting and linting:

```bash
npm run format  # Format code
npm run lint    # Lint and fix issues
```

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/) format:

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `style:` - Code style changes (formatting, etc.)
- `refactor:` - Code refactoring
- `test:` - Adding or updating tests
- `chore:` - Maintenance tasks

Examples:
```
feat: add support for batch write operations
fix: resolve type error in condition expressions
docs: update getting started guide
test: add unit tests for update expressions
```

## Testing

### Writing Tests

- Write unit tests for all new functionality
- Use descriptive test names that explain what is being tested
- Follow the Arrange-Act-Assert pattern
- Mock external dependencies (DynamoDB client, etc.)

### Running Tests

```bash
# Unit tests
npm test                    # Run once
npm run test:watch          # Watch mode
npm run test:coverage       # With coverage
```

### Test Structure

```typescript
import { describe, it, expect } from 'vitest';

describe('FeatureName', () => {
  it('should do something specific', () => {
    // Arrange
    const input = 'test';
    
    // Act
    const result = doSomething(input);
    
    // Assert
    expect(result).toBe('expected');
  });
});
```

### Integration Tests

Integration tests interact with real AWS services. Ensure you have valid AWS credentials configured.

To stand up the necessary resources for integration tests, run:
```bash
AWS_PROFILE=YOUR_AWS_PROFILE npm run test:integration:cdk deploy integration-tests
```

This will provision a CDK stack with the required DynamoDB tables. You can then run the integration tests with:
```bash
AWS_PROFILE=YOUR_AWS_PROFILE npm run test:integration
```

## Documentation

Documentation is built with [Astro](https://astro.build/) and hosted at [dynamodocumentbuilder.com](https://dynamodocumentbuilder.com).

To work on documentation:
```bash
npm run docs
```

## Questions?

If you have questions about contributing, feel free to:

- Open a [GitHub Discussion](https://github.com/brandonburrus/dynamo-document-builder/discussions)
- Comment on an existing issue
- Reach out to the maintainers

