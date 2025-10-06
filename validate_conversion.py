#!/usr/bin/env python3
"""
Simple validation script for the generated Fabric PySpark notebook.
This script performs basic syntax and import validation without requiring Spark runtime.
"""

import ast
import sys
import os

def validate_python_syntax(file_path):
    """Validate that the Python file has correct syntax."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Parse the AST to check syntax
        ast.parse(content)
        print(f"✓ Syntax validation passed for {file_path}")
        return True
    except SyntaxError as e:
        print(f"✗ Syntax error in {file_path}: {e}")
        return False
    except Exception as e:
        print(f"✗ Error reading {file_path}: {e}")
        return False

def check_required_imports(file_path):
    """Check that required PySpark imports are present."""
    required_imports = [
        'pyspark.sql.functions',
        'pyspark.sql.window',
        'delta.tables'
    ]
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        missing_imports = []
        for imp in required_imports:
            if imp not in content:
                missing_imports.append(imp)
        
        if missing_imports:
            print(f"✗ Missing imports in {file_path}: {missing_imports}")
            return False
        else:
            print(f"✓ All required imports present in {file_path}")
            return True
    except Exception as e:
        print(f"✗ Error checking imports in {file_path}: {e}")
        return False

def validate_class_structure(file_path):
    """Validate that the main class and required methods are present."""
    required_methods = [
        'df_json_files',
        'df_old_records', 
        'df_new_records',
        'df_distinct_data_point_names',
        'run'
    ]
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Check for main class
        if 'class DfL2TextValues:' not in content:
            print(f"✗ Main class 'DfL2TextValues' not found in {file_path}")
            return False
        
        missing_methods = []
        for method in required_methods:
            if f'def {method}(' not in content:
                missing_methods.append(method)
        
        if missing_methods:
            print(f"✗ Missing required methods in {file_path}: {missing_methods}")
            return False
        else:
            print(f"✓ All required methods present in {file_path}")
            return True
    except Exception as e:
        print(f"✗ Error checking class structure in {file_path}: {e}")
        return False

def main():
    """Main validation function."""
    print("Starting validation of generated Fabric PySpark notebook...")
    
    fabric_notebook = "/home/runner/work/TestRepo/TestRepo/Fabric_nb_df_l2_text_values.py"
    
    if not os.path.exists(fabric_notebook):
        print(f"✗ File not found: {fabric_notebook}")
        sys.exit(1)
    
    # Run all validations
    validations = [
        validate_python_syntax(fabric_notebook),
        check_required_imports(fabric_notebook),
        validate_class_structure(fabric_notebook)
    ]
    
    if all(validations):
        print("\n✓ All validations passed! The generated Fabric notebook appears to be correctly structured.")
        return True
    else:
        print("\n✗ Some validations failed. Please review the generated code.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)