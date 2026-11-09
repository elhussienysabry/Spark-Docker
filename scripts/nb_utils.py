#!/usr/bin/env python3
"""
Notebook generation utility providing standard educational notebook construction.
Ensures every notebook strictly follows the 7-part pedagogical framework.
"""
import json
import os

class NotebookBuilder:
    def __init__(self, title, module_name, notebook_name, app_name="SparkLab"):
        self.title = title
        self.module_name = module_name
        self.notebook_name = notebook_name
        self.app_name = app_name
        self.cells = []

    def _split_lines(self, text):
        lines = text.split("\n")
        return [l + "\n" for l in lines[:-1]] + ([lines[-1]] if lines[-1] else [])

    def add_markdown(self, md_text):
        self.cells.append({
            "cell_type": "markdown",
            "metadata": {},
            "source": self._split_lines(md_text.strip())
        })
        return self

    def add_code(self, code_text):
        self.cells.append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": self._split_lines(code_text.strip())
        })
        return self

    def add_header(self, objectives_dict):
        """
        Part 1: Title & Learning Objectives
        """
        md = f"""#  {self.title}

**Module**: {self.module_name}  
**Notebook**: {self.notebook_name}  
**Environment**: Apache Spark 4.2.1 · Standalone Cluster · PySpark · Python 3.13 · Java 21  

---

##  Learning Objectives

- **What will I learn?**: {objectives_dict.get('what', '')}
- **Why does this topic exist?**: {objectives_dict.get('why', '')}
- **What problem does it solve?**: {objectives_dict.get('problem', '')}
- **Where is it used in real-world Data Engineering?**: {objectives_dict.get('real_world', '')}

---
"""
        self.add_markdown(md)
        return self

    def add_session_init(self, extra_configs=None):
        """
        Standard SparkSession builder connected to Docker Standalone Cluster
        """
        config_lines = ""
        if extra_configs:
            for k, v in extra_configs.items():
                config_lines += f'    .config("{k}", "{v}")\n'

        code = f"""from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time

spark = (
    SparkSession.builder
    .appName("{self.app_name}")
    .master("spark://spark-master:7077")
    .config("spark.driver.host", "jupyter")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "file:///opt/spark-events")
    .config("spark.sql.warehouse.dir", "file:///opt/spark-data/warehouse")
    .config("spark.hadoop.fs.permissions.umask-mode", "000")
    .config("spark.sql.shuffle.partitions", "8")
{config_lines}    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f" SparkSession initialized successfully!")
print(f"Spark Version:      {{spark.version}}")
print(f"Master URL:         {{spark.sparkContext.master}}")
print(f"App Name:           {{spark.sparkContext.appName}}")
print(f"Default Parallelism: {{spark.sparkContext.defaultParallelism}}")
print(f"Spark UI available at: http://localhost:4040")
"""
        self.add_markdown("###  1. Initialize SparkSession on Standalone Cluster\n\nWe connect to `spark://spark-master:7077` with the driver configured to bind to container hostname `jupyter`.")
        self.add_code(code)
        return self

    def add_experiment(self, exp_dict):
        """
        Part 5: Experiments
        """
        md = f"""---

##  Experiment: {exp_dict['title']}

- **1. What are we testing?**: {exp_dict['testing']}
- **2. What do we expect?**: {exp_dict['expect']}
"""
        self.add_markdown(md)
        self.add_code(exp_dict['code'])
        analysis_md = f"""**3. What happened?**: {exp_dict['happened']}  
**4. Why did it happen?**: {exp_dict['why']}  
**5. Practical Engineering Lesson**: {exp_dict['takeaway']}
"""
        self.add_markdown(analysis_md)
        return self

    def add_exercises(self, exercises_dict):
        """
        Part 6: Exercises (Beginner, Intermediate, Advanced) with separate solutions
        """
        md = f"""---

##  Practical Exercises

###  Exercise 1 (Beginner): {exercises_dict.get('b_title', 'Exercise 1')}
**Problem Statement**: {exercises_dict.get('b_desc', '')}  
**Input / Context**: {exercises_dict.get('b_input', 'Provided dataset')}
"""
        self.add_markdown(md)
        self.add_code(exercises_dict.get('b_starter', '# Write your solution for Exercise 1 here\n'))

        md_int = f"""###  Exercise 2 (Intermediate): {exercises_dict.get('i_title', 'Exercise 2')}
**Problem Statement**: {exercises_dict.get('i_desc', '')}  
**Input / Context**: {exercises_dict.get('i_input', 'Provided dataset')}
"""
        self.add_markdown(md_int)
        self.add_code(exercises_dict.get('i_starter', '# Write your solution for Exercise 2 here\n'))

        md_adv = f"""###  Exercise 3 (Advanced): {exercises_dict.get('a_title', 'Exercise 3')}
**Problem Statement**: {exercises_dict.get('a_desc', '')}  
**Input / Context**: {exercises_dict.get('a_input', 'Provided dataset')}
"""
        self.add_markdown(md_adv)
        self.add_code(exercises_dict.get('a_starter', '# Write your solution for Exercise 3 here\n'))

        # Solutions in collapsed or designated section
        solutions_md = f"""---

##  Reference Solutions

<details>
<summary><b>Click to expand full solutions</b></summary>

#### Solution 1 ({exercises_dict.get('b_title', 'Exercise 1')}):
```python
{exercises_dict.get('b_sol', '# Solution 1')}
```

#### Solution 2 ({exercises_dict.get('i_title', 'Exercise 2')}):
```python
{exercises_dict.get('i_sol', '# Solution 2')}
```

#### Solution 3 ({exercises_dict.get('a_title', 'Exercise 3')}):
```python
{exercises_dict.get('a_sol', '# Solution 3')}
```

</details>
"""
        self.add_markdown(solutions_md)
        return self

    def add_summary_and_teardown(self, summary_dict):
        """
        Part 7: Summary & Teardown
        """
        takeaways_list = "\n".join([f"- **{k}**: {v}" for k, v in summary_dict.get('takeaways', {}).items()])
        terms_list = "\n".join([f"- `{k}`: {v}" for k, v in summary_dict.get('terms', {}).items()])
        mistakes_list = "\n".join([f"-  **{k}**: {v}" for k, v in summary_dict.get('mistakes', {}).items()])
        questions_list = "\n".join([f"-  **Q: {q}**\n  - *A*: {a}" for q, a in summary_dict.get('questions', {}).items()])

        md = f"""---

##  Module Summary & Interview Guide

###  Key Takeaways
{takeaways_list}

###  Essential Technical Terms
{terms_list}

###  Common Engineering Mistakes
{mistakes_list}

###  Interview Questions on This Topic
{questions_list}

###  What to Study Next
{summary_dict.get('next_steps', 'Proceed to the next notebook in the curriculum sequence.')}

---

###  Tear Down SparkSession
Always close your session when finishing to release executor threads and driver ports.
"""
        self.add_markdown(md)
        self.add_code("spark.stop()\nprint('SparkSession successfully stopped.')")
        return self

    def save(self, filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        data = {
            "cells": self.cells,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3 (ipykernel)",
                    "language": "python",
                    "name": "python3"
                },
                "language_info": {
                    "codemirror_mode": {
                        "name": "ipython",
                        "version": 3
                    },
                    "file_extension": ".py",
                    "mimetype": "text/x-python",
                    "name": "python",
                    "nbconvert_exporter": "python",
                    "pygments_lexer": "ipython3",
                    "version": "3.13.18"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 5
        }
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=1)
        print(f" Generated: {filepath} ({len(self.cells)} cells)")
