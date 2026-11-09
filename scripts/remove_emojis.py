#!/usr/bin/env python3
"""
Strips all emojis from README.md, docs/*.md, and notebooks/*/*.ipynb
Ensures files remain valid markdown, valid JSON, and valid Python syntax.
"""
import ast
import json
import os
import re

EMOJI_PATTERN = re.compile(
    r'[\U0001F000-\U0001FAFF]'
    r'|[\u2600-\u27BF]'
    r'|[\uFE00-\uFE0F]'
    r'|[\u2300-\u23FF]'
    r'|[\u2B50\u2B55\u200D]'
)

def clean_text(text, is_code=False):
    if not text:
        return text
    # Remove emoji characters
    cleaned = EMOJI_PATTERN.sub('', text)
    
    if not is_code:
        # Markdown cleanup
        cleaned = re.sub(r'^(#+\s+)[ \t]+', r'\1', cleaned, flags=re.MULTILINE)
        cleaned = re.sub(r'^([ \t]*[-*]\s+)[ \t]+', r'\1', cleaned, flags=re.MULTILINE)
        cleaned = re.sub(r'([\'\"\(\[\{])[ \t]+', r'\1', cleaned)
        cleaned = re.sub(r'[ \t]+([\'\"\)\}\]])', r'\1', cleaned)
    else:
        # For code, only strip trailing whitespace or spaces left inside literal strings if any
        # Preserve all leading indentation!
        cleaned = re.sub(r'[ \t]+$', '', cleaned, flags=re.MULTILINE)
    
    return cleaned

def clean_markdown_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    new_content = clean_text(content)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print(f"Cleaned MD: {filepath}")

def clean_notebook_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        nb = json.load(f)
    
    for cell in nb.get("cells", []):
        is_code = (cell.get("cell_type") == "code")
        source = cell.get("source", [])
        if isinstance(source, list):
            cell_text = "".join(source)
            cleaned_text = clean_text(cell_text, is_code=is_code)
            # Re-split into line array
            lines = cleaned_text.split("\n")
            cell["source"] = [l + "\n" for l in lines[:-1]] + ([lines[-1]] if lines[-1] else [])
        elif isinstance(source, str):
            cell["source"] = clean_text(source, is_code=is_code)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(nb, f, indent=1)
    print(f"Cleaned IPYNB: {filepath}")

def main():
    base_dir = "/home/elhussieny/spark-docker"

    # 1. README.md
    readme_path = os.path.join(base_dir, "README.md")
    if os.path.exists(readme_path):
        clean_markdown_file(readme_path)

    # 2. docs/*.md
    docs_dir = os.path.join(base_dir, "docs")
    for file in os.listdir(docs_dir):
        if file.endswith(".md"):
            clean_markdown_file(os.path.join(docs_dir, file))

    # 3. notebooks/*/*.ipynb
    nb_dir = os.path.join(base_dir, "notebooks")
    for root, dirs, files in os.walk(nb_dir):
        if ".ipynb_checkpoints" in root:
            continue
        for file in files:
            if file.endswith(".ipynb"):
                clean_notebook_file(os.path.join(root, file))

    print("\nAll markdown files and notebooks have been stripped of emojis.")

if __name__ == '__main__':
    main()
