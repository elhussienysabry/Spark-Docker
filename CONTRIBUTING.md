# Contributing to Apache Spark Learning Lab

Thank you for your interest in contributing to the Apache Spark Learning Lab. This project aims to provide the most rigorous, hands-on, and technically accurate Apache Spark learning curriculum and engineering environment available.

Contributions of all types are welcome: bug fixes, notebook improvements, documentation enhancements, additional performance experiments, and new production scenarios.

---

## Code of Conduct

All contributors and participants are expected to adhere to the [Code of Conduct](CODE_OF_CONDUCT.md). Please maintain a respectful, welcoming, and collaborative environment.

---

## Getting Started

1. Fork the repository on GitHub.
2. Clone your fork locally:
   ```bash
   git clone https://github.com/<your-username>/Spark-Docker.git
   cd Spark-Docker
   ```
3. Copy the example environment file if you wish to adjust default ports or worker resources:
   ```bash
   cp .env.example .env
   ```
4. Start the Spark Standalone cluster:
   ```bash
   docker compose up -d
   ```
5. Verify that all 5 services are healthy and running:
   ```bash
   docker compose ps
   ```

---

## Development Standards

### 1. Notebook Pedagogical Structure

Every curriculum notebook must follow the standard 7-part pedagogical structure:

1. Conceptual Foundation: Deep theoretical breakdown, mathematical intuition, and core mechanics.
2. Architecture and Internals: Visual ASCII/Mermaid flowcharts, memory layouts, Driver/Worker interaction.
3. Production Scenario: Real-world engineering context, failure mode analysis, architectural trade-offs.
4. Hands-on Code Lab: Executable, reproducible PySpark code with clear explanations.
5. Spark UI and Diagnostics: Step-by-step guidance on inspecting the Web UI and Event History.
6. Practical Exercises: Multi-level challenge exercises with starter templates and hints.
7. Key Takeaways: Bulleted engineering summary and production rules of thumb.

### 2. Code Quality and Validation

Before committing changes to any notebook or script, run the automated validation suite:

```bash
python3 scripts/validate_notebooks.py
```

The validator verifies:
- JSON format integrity across all Jupyter notebooks.
- Python syntax correctness in every code cell via Abstract Syntax Tree (AST) parsing.
- Prevention of broken imports, unescaped characters, or malformed cell metadata.

All pull requests must pass this validation check without errors.

### 3. Writing and Formatting Guidelines

- Zero Emojis: Maintain a strictly professional, technical publication style. Do not use emojis in notebooks, code comments, documentation, commit messages, or pull request descriptions.
- Markdown Standards: Use GitHub Flavored Markdown (GFM). Use fenced code blocks with language identifiers.
- Paths: Always use relative paths from the repository root or notebook working directory (for example, `data/retail/orders.csv`). Output files should be directed to `data/warehouse/` or `data/output/` which are excluded by `.gitignore`.
- Python Version Compatibility: PySpark 4.2.1 running on Python 3.13 and Java 21. Ensure all code examples are fully compatible with this runtime.

---

## Pull Request Process

1. Create a feature branch:
   ```bash
   git checkout -b feature/your-topic-name
   ```
2. Make your modifications, adhering to the curriculum standards.
3. Run the validation suite:
   ```bash
   python3 scripts/validate_notebooks.py
   ```
4. Test your changes against the running Docker cluster (JupyterLab at `http://localhost:8888/?token=spark`).
5. Ensure no runtime artifacts (event logs, warehouse data, pycache) are staged:
   ```bash
   git status
   ```
6. Commit your changes with descriptive, conventional commit messages:
   ```bash
   git commit -m "docs: add adaptive query execution skew join lab"
   ```
7. Push your branch to GitHub and open a Pull Request against the `main` branch.
8. Describe your changes clearly in the PR description, referencing any relevant issues.

---

## Reporting Issues

If you encounter bugs, broken links, or execution errors:
1. Search existing issues to ensure the problem has not already been reported.
2. If opening a new issue, use the Bug Report template and include:
   - Operating system and Docker version.
   - Exact notebook name and cell index.
   - Full stack trace or error output.
   - Steps to reproduce.
