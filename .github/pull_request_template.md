## Description
Please include a summary of the changes and the related issue (if applicable). Please also include relevant motivation and context.

Fixes # (issue)

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New notebook or curriculum module
- [ ] Documentation improvement or architectural update
- [ ] Cluster infrastructure or Docker configuration improvement
- [ ] Dataset or validation script enhancement

## Validation Checklist
- [ ] I have executed `python scripts/validate_notebooks.py` and all notebooks pass syntax and JSON validation.
- [ ] I have executed `python scripts/check_links.py` and all local links resolve.
- [ ] I have tested the notebooks against the local Docker Spark cluster.
- [ ] I have verified that no emojis were introduced into markdown or notebook text.
- [ ] I have confirmed that no generated runtime files (event logs, warehouse outputs, pycache) are staged.
- [ ] All new and existing tests pass.
